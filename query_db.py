import os
import sys
from pgvector.psycopg2 import register_vector
import psycopg2.extras
import psycopg2
from sentence_transformers import SentenceTransformer

# Initialize the transformer model
model = SentenceTransformer('multi-qa-MiniLM-L6-cos-v1', device='cuda')
model.max_seq_length = 512

# Check if the query is provided as a command-line argument
if len(sys.argv) < 2:
    print("Please provide a query as a command-line argument.")
    sys.exit(1)

query = sys.argv[1]  # Get the query from command-line argument

embedding = model.encode(query)

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

# Get NN to embedding
# cursor.execute('SELECT * FROM wikipedia ORDER BY embedding <-> %s LIMIT 5', (embedding,))
cursor.execute(
    'SELECT chunk FROM wikipedia ORDER BY embedding <-> %s LIMIT 5', (embedding,))
rows = cursor.fetchall()

for row in rows:
    print(row[0])
    print()

# Close the cursor and connection
cursor.close()
db_connection.close()
