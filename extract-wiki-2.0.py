import os
import io
import bz2
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import lxml.etree as etree
from multiprocessing import Pool
from tqdm import tqdm
from bz2 import BZ2Decompressor
from typing import List, Generator
import mwparserfromhell

# Wikipedia dump version
DUMP_VERSION = '20230301'

# File paths for input and output files
ARTICLES_PATH = f'enwiki-{DUMP_VERSION}-pages-articles-multistream1.xml.bz2'
INDEX_PATH = f'enwiki-{DUMP_VERSION}-pages-articles-multistream-index1.txt.bz2'
CLEAN_INDEX_PATH = f'enwiki-{DUMP_VERSION}-pages-articles-multistream-index1.txt'
OUTPUT_PARQUET_PATH = 'wiki_parquet/'

# Processing parameters
NUM_PROCESSORS = 16
NUM_PARALLEL_BLOCKS = 20


def get_article_offsets(index_path: str, clean_index_path: str) -> List[int]:
    """
    Returns the offsets of the start of each Wikipedia article within the bz2 file.
    If the cleaned index file already exists, the function simply reads the offsets from this file.
    Otherwise, it calculates the offsets from the original index file and writes them to the cleaned index file.
    """
    if not os.path.isfile(clean_index_path):
        article_offsets = []
        last_offset = None
        with open(index_path, 'rb') as f:
            compressed_data = bz2.decompress(f.read()).split(b'\n')
            if compressed_data[-1] == b'':
                compressed_data = compressed_data[:-1]
            for line in tqdm(compressed_data, desc="Processing Article Offsets", total=len(compressed_data)):
                offset = line.decode().split(':', 1)[0]
                if last_offset != offset:
                    last_offset = offset
                    article_offsets.append(int(offset))

        with open(clean_index_path, 'w') as f:
            f.write(','.join([str(i) for i in article_offsets]))
    else:
        with open(clean_index_path, 'r') as f:
            article_offsets = [int(idx) for idx in f.read().split(',')]

    return article_offsets


def extract_article_data(article_path: str, offset_list: List[int]) -> Generator[bytes, None, None]:
    """
    Extracts the raw byte data for each Wikipedia article from the bz2 file.
    """
    with open(article_path, "rb") as f:
        last_offset = offset_list[0]
        f.read(last_offset)
        for next_offset in offset_list[1:]:
            offset = next_offset - last_offset
            last_offset = next_offset
            yield f.read(offset)


def parse_article_data(byte_string_compressed: bytes) -> pd.DataFrame:
    """
    Parses the raw byte data of a Wikipedia article and returns a pandas DataFrame containing the article ID, title, and text.
    """
    def _extract_text(list_xml_el):
        return [el.text for el in list_xml_el]

    def _extract_id(list_xml_el):
        return [int(el.text) for el in list_xml_el]

    decompressor = BZ2Decompressor()
    byte_string = decompressor.decompress(byte_string_compressed)
    doc = etree.parse(io.BytesIO(b'<root> ' + byte_string + b' </root>'))

    id_column = _extract_id(doc.xpath('*/id'))
    title_column = _extract_text(doc.xpath('*/title'))
    article_column = _extract_text(doc.xpath('*/revision/text'))

    df = pd.DataFrame([id_column, title_column, article_column], index=[
                      'index', 'title', 'article']).T
    df['index'] = df['index'].astype(np.int32)
    return df


def partition_list(input_list: List, chunk_size: int) -> Generator[List, None, None]:
    """
    Splits a list into smaller chunks of a given size. 
    Returns a generator that yields these chunks one by one.
    """
    for i in range(0, len(input_list), chunk_size):
        yield input_list[i:i+chunk_size]


def clean_wiki_text(raw_text):
    # Clean Wikipedia text
    wikicode = mwparserfromhell.parse(raw_text)
    intermediate_text = wikicode.strip_code()

    # Split text into lines and remove consecutive empty lines
    lines = intermediate_text.split('\n')
    clean_lines = [line for line, prev_line in zip(
        lines[1:], lines) if line.strip() or prev_line.strip()]

    clean_text = '\n'.join(clean_lines)
    return clean_text


def split_text_into_chunks(text):
    # Split text into paragraphs
    paragraphs = text.split("\n")
    chunks = []
    current_chunk = ""

    # Maximum words per chunk (75% of 512)
    max_words_per_chunk = 350

    # Create chunks that don't exceed the word limit
    for paragraph in paragraphs:
        num_words_current = len(current_chunk.split())
        num_words_new = len(paragraph.split())

        if num_words_current + num_words_new > max_words_per_chunk:
            current_chunk = current_chunk.strip()
            if current_chunk:
                chunks.append(current_chunk)
            current_chunk = paragraph
        else:
            current_chunk += "\n" + paragraph

    # Add the last chunk if it's not empty
    if current_chunk.strip():
        chunks.append(current_chunk)

    return chunks


def process_articles_in_parallel(list_bytes: List[bytes]) -> None:
    """
    Processes a list of raw byte data for Wikipedia articles in parallel using multiple processors.
    Writes the processed data to Parquet files.
    """
    df_list = []
    for article in tqdm(list_bytes, desc="Processing Articles", total=len(list_bytes)):
        df = parse_article_data(article)
        df = df[~df['article'].apply(
            lambda x: x.lower().startswith('#redirect'))]
        df['article'] = df['article'].apply(clean_wiki_text)
        df['chunks'] = df['article'].apply(split_text_into_chunks)
        df = df.explode('chunks').reset_index(drop=True)
        df.drop(columns=['article'], inplace=True)
        df_list.append(df)

    output_file_path = os.path.join(
        OUTPUT_PARQUET_PATH, '{:08d}.parquet'.format(df_list[0]['index'].values[0]))

    df_combined = pd.concat(df_list, ignore_index=True)
    df_combined.to_parquet(output_file_path, compression='snappy', index=False)
    del df_combined


# Main process
articles_queue = []
article_offsets = get_article_offsets(INDEX_PATH, CLEAN_INDEX_PATH)
for byte_string in tqdm(extract_article_data(ARTICLES_PATH, article_offsets), desc="Offsets Loaded", total=len(article_offsets)):
    if len(articles_queue) < NUM_PROCESSORS * NUM_PARALLEL_BLOCKS:
        articles_queue.append(byte_string)
    else:
        with Pool(processes=NUM_PROCESSORS) as pool:
            tuple(pool.imap_unordered(process_articles_in_parallel,
                  partition_list(articles_queue, NUM_PARALLEL_BLOCKS)))
        for el in articles_queue:
            del el
        articles_queue.clear()

with Pool(processes=NUM_PROCESSORS) as pool:
    tuple(pool.imap_unordered(process_articles_in_parallel,
          partition_list(articles_queue, NUM_PARALLEL_BLOCKS)))
for el in articles_queue:
    del el
articles_queue.clear()

print("Done.")
