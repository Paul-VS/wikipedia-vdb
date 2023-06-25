import os
from pgvector.psycopg2 import register_vector
import psycopg2.extras
import psycopg2
from sentence_transformers import SentenceTransformer
import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm
import multiprocessing as mp
import time
import json
import openai
from dotenv import load_dotenv

table_name = "svelte"  # Set your desired table name here


# Start the timer
start_time = time.time()

# # Read the parquet file into a pandas DataFrame
# file_path = 'wiki_parquet/00000012.parquet'
# parquet_file = pq.ParquetFile(file_path)
# df = parquet_file.read_row_group(0).to_pandas()

# Read the JSON file into a pandas DataFrame
with open('output.json', 'r') as file:
    data = json.load(file)
formatted_data = []
for title, chunks in data.items():
    for chunk in chunks:
        formatted_data.append({"title": title, "chunks": chunk})
df = pd.DataFrame(formatted_data)


# OPTION Use ADA-002 API for embeddings
# load_dotenv()
# openai.api_key = os.environ.get("OPENAI_API_KEY")
# response = openai.Embedding.create(
#     model="text-embedding-ada-002",
#     input=df['chunks'].tolist()
# )
# embeddings = [item['embedding'] for item in response['data']]
# df['embedding'] = list(embeddings)

# Initialize the transformer model
# model = SentenceTransformer('multi-qa-MiniLM-L6-cos-v1', device='cuda')
model = SentenceTransformer(
    'sentence-transformers/msmarco-distilbert-base-tas-b', device='cuda')
model.max_seq_length = 512

# Define the batch size
batch_size = 64

# Extract the chunks to a list
chunks = df['chunks'].tolist()

# Compute embeddings for each chunk in the DataFrame
embeddings = model.encode(chunks, batch_size=batch_size,
                          convert_to_numpy=True, show_progress_bar=True)

# Assign the embeddings back to the DataFrame
df['embedding'] = list(embeddings)

print(df)

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

# Drop the table
cursor.execute(f"""
   DROP TABLE IF EXISTS {table_name}
""")
db_connection.commit()

# Create the table
cursor.execute(f"""
   CREATE TABLE IF NOT EXISTS {table_name} (
       id SERIAL PRIMARY KEY,
       title TEXT NOT NULL,
       chunk TEXT NOT NULL,
       embedding VECTOR NOT NULL
   )
""")
db_connection.commit()

# Prepare data for insertion
data_for_insertion = ((row.title, row.chunks, row.embedding)
                      for _, row in df.iterrows())

# Wrap your generator with tqdm for a progress bar
data_for_insertion = tqdm(
    data_for_insertion, desc="Uploading to database", total=df.shape[0])

# Insert data into the table
psycopg2.extras.execute_values(
    cursor,
    f"""
    INSERT INTO {table_name} (title, chunk, embedding) VALUES %s
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
elapsed_time = end_time - start_time
print(f"Script execution time: {elapsed_time} seconds")
