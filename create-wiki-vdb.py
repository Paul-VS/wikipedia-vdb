import os
from pgvector.psycopg2 import register_vector
import psycopg2.extras
import psycopg2
from sentence_transformers import SentenceTransformer
import pandas as pd
import pyarrow.parquet as pq
import mwparserfromhell
from tqdm import tqdm
import multiprocessing as mp
import time

# Start the timer
start_time = time.time()

# Define the path to the parquet file
file_path = 'wiki_parquet/00000010.parquet'

# Read the parquet file into a pandas DataFrame
parquet_file = pq.ParquetFile(file_path)
df = parquet_file.read_row_group(0).to_pandas()

# Drop rows where articles only redirect
df = df[~df['article'].apply(lambda x: x.lower().startswith('#redirect'))]


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
            chunks.append(current_chunk)
            current_chunk = paragraph
        else:
            current_chunk += "\n" + paragraph

    # Add the last chunk if it's not empty
    if current_chunk.strip():
        chunks.append(current_chunk)

    return chunks


def process_article(row):
    _, row_data = row
    title, article = row_data.title, row_data.article
    cleaned_article = clean_wiki_text(article)
    chunked_article = split_text_into_chunks(cleaned_article)
    return pd.DataFrame([(title, chunk) for chunk in chunked_article], columns=['title', 'chunk'])


with mp.Pool(processes=mp.cpu_count()) as pool:
    chunked_articles_df_list = list(tqdm(
        pool.imap(process_article, df.iterrows(), chunksize=400), desc="Cleaning and chunking articles", total=df.shape[0]))

chunked_articles_df = pd.concat(chunked_articles_df_list, ignore_index=True)

# print(chunked_articles_df)

# Initialize the transformer model
model = SentenceTransformer('multi-qa-MiniLM-L6-cos-v1', device='cuda')
model.max_seq_length = 512

# Define the batch size
batch_size = 32


def compute_embeddings(rows):
    # Compute embeddings for each chunk in the DataFrame
    chunks = rows['chunk'].tolist()
    embeddings = model.encode(chunks)
    return embeddings


# Group chunks into batches
batches = [chunked_articles_df[i:i+batch_size]
           for i in range(0, len(chunked_articles_df), batch_size)]

# Compute embeddings for each batch
embeddings = []
for batch in tqdm(batches, desc="Computing embeddings in batches"):
    batch_embeddings = compute_embeddings(batch)
    embeddings.extend(batch_embeddings)


# Assign the embeddings back to the DataFrame
chunked_articles_df['embedding'] = embeddings

# print(chunked_articles_df)

# Set up connection parameters
db_connection_params = {
    "host": "localhost",
    "database": "vector_db",
    "user": os.environ["PG_VECTOR_DB_USER"],
    "password": os.environ["PG_VECTOR_DB_PASSWORD"],
}

# Establish a connection to the database
db_connection = psycopg2.connect(**db_connection_params)

# Register the vector type with your connection
register_vector(db_connection)

# Create a cursor object
cursor = db_connection.cursor()

# # Drop test table
# cursor.execute("""
#     DROP TABLE IF EXISTS wikipedia
# """)
# db_connection.commit()
#
# # Create the table
# cursor.execute("""
#     CREATE TABLE IF NOT EXISTS wikipedia (
#         id SERIAL PRIMARY KEY,
#         title TEXT NOT NULL,
#         chunk TEXT NOT NULL,
#         embedding VECTOR NOT NULL
#     )
# """)
# db_connection.commit()

# Prepare data for insertion
data_for_insertion = ((row.title, row.chunk, row.embedding)
                      for _, row in chunked_articles_df.iterrows())

# Wrap your generator with tqdm for a progress bar
data_for_insertion = tqdm(
    data_for_insertion, desc="Uploading to database", total=chunked_articles_df.shape[0])

# Insert data into the table
psycopg2.extras.execute_values(
    cursor,
    """
    INSERT INTO wikipedia (title, chunk, embedding) VALUES %s
    """,
    data_for_insertion,
    template="(%s, %s, %s::vector)"
)

db_connection.commit()

# Close the cursor and the connection
cursor.close()
db_connection.close()


# End the timer
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

# Print the elapsed time
print(f"Script execution time: {elapsed_time} seconds")
