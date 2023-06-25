"""
A script to fetch markdown files from a Github repository, split them into chunks at heading boundaries,
and store the results in a JSON file.
"""

import re
import base64
import os
import json
import requests
from bs4 import BeautifulSoup
from markdown import markdown
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('script.log'),
        logging.StreamHandler()
    ]
)

load_dotenv()  # Load environment variables from .env.

# Github API base URL
GITHUB_API = 'https://api.github.com'

# GitHub username, repository and folder to scrape
USERNAME = 'sveltejs'
# REPO = 'svelte'
REPO = 'kit'
PATH = 'documentation/docs/'

# Personal access token
TOKEN = os.getenv('GITHUB_TOKEN')


def get_files_recursive(path=PATH):
    """
    Recursively fetches markdown files from a given path in the repository.

    Parameters:
    path (str): The path in the repository from where to start fetching markdown files.

    Returns:
    list: A list of paths to markdown files.
    """
    url = f'{GITHUB_API}/repos/{USERNAME}/{REPO}/contents/{path}'
    logging.info(f'Fetching files from {url}')
    headers = {'Authorization': f'token {TOKEN}'}
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        files = r.json()
        if isinstance(files, dict) and 'message' in files:
            raise Exception(files['message'])
    except Exception as e:
        logging.error(f'Failed to fetch files from {path}. Reason: {str(e)}')
        return []

    markdown_files = []
    for file in files:
        if file['type'] == 'dir':
            markdown_files += get_files_recursive(file['path'])
        elif file['name'].endswith('.md'):
            markdown_files.append(file['path'])

    return markdown_files


def download_file(path):
    """
    Downloads and decodes a file from a given path in the repository.

    Parameters:
    path (str): The path to the file in the repository.

    Returns:
    str: The decoded content of the file.
    """
    logging.info(f'Downloading file {path}')
    url = f'{GITHUB_API}/repos/{USERNAME}/{REPO}/contents/{path}'
    headers = {'Authorization': f'token {TOKEN}'}
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        file_content = r.json()['content']
        encoding = r.json()['encoding']
        if encoding != 'base64':
            raise Exception(f'Unexpected encoding: {encoding}')
    except Exception as e:
        logging.error(f'Failed to download file {path}. Reason: {str(e)}')
        return None

    try:
        return base64.b64decode(file_content).decode()
    except Exception as e:
        logging.error(f'Failed to decode file {path}. Reason: {str(e)}')
        return None


def split_markdown(content):
    """
    Splits the content of a markdown file into chunks at each heading, 
    merges them as long as they are not longer than 350 words.

    Parameters:
    content (str): The content of a markdown file.

    Returns:
    list: A list of chunks.
    """
    if content is None:
        return []

    logging.info('Splitting markdown content into chunks')
    # Split the content at each heading using regular expressions
    chunks = re.split(r'(^#+\s.*)', content, flags=re.MULTILINE)[1:]

    # Merge the heading with the following content
    chunks = [heading + chunk for heading,
              chunk in zip(chunks[0::2], chunks[1::2])]

    # # Merge chunks as long as they are not longer than 250 words
    # merged_chunks = []
    # current_chunk = ""
    # for chunk in chunks:
    #     # If adding the new chunk won't exceed the word limit, add it
    #     if len((current_chunk + chunk).split()) <= 250:
    #         current_chunk += " " + chunk
    #     else:
    #         # If it would, add the current_chunk to the list and start a new one
    #         merged_chunks.append(current_chunk)
    #         current_chunk = chunk

    # # Add the last chunk if it's not empty
    # if current_chunk:
    #     merged_chunks.append(current_chunk)

    return chunks


def main():
    """
    Main function. Fetches markdown files from the repository, splits them into chunks,
    and stores the results in a JSON file.
    """
    markdown_files = get_files_recursive()
    data = {}

    for file_path in markdown_files:
        url = f'{GITHUB_API}/repos/{USERNAME}/{REPO}/contents/{file_path}'
        logging.info(f'Processing file at {url}')
        content = download_file(file_path)
        chunks = split_markdown(content)

        if chunks:
            data[url] = chunks

    with open('output.json', 'w') as f:
        logging.info('Writing data to output.json')
        json.dump(data, f, indent=4)

    logging.info('Done.')


if __name__ == '__main__':
    main()
