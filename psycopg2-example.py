import psycopg2
from psycopg2 import sql
import psycopg2.extras
from io import StringIO
from pgvector.psycopg2 import register_vector

# Set up connection parameters
db_connection_params = {
    "host": "localhost",
    "database": "vector_db",
    "user": "postgres",
    "password": "password"
}

# Establish a connection to the database
db_connection = psycopg2.connect(**db_connection_params)

# Register the vector type with your connection
register_vector(db_connection)

# Create a cursor object
cursor = db_connection.cursor()

# Drop test table
cursor.execute("""
    DROP TABLE IF EXISTS wikipedia 
""")
db_connection.commit()

# Create the table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS wikipedia (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        chunk TEXT NOT NULL,
        embedding VECTOR NOT NULL
    )
""")
db_connection.commit()

# Test data
test_data = [
    {"title": "Article 1", "chunk": "This is the content of Article 1.",
        "embedding": [1, 2, 3]},
    {"title": "Article 2", "chunk": "This is the content of Article 2.",
        "embedding": [4, 5, 6]},
    {"title": "Article 3", "chunk": "This is the content of Article 3.",
        "embedding": [7, 8, 9]},
    # Add more test data as needed
]

# Insert test data into the table
for data in test_data:
    insert_query = "INSERT INTO wikipedia (title, chunk, embedding) VALUES (%s, %s, %s)"
    cursor.execute(
        insert_query, (data["title"], data["chunk"], data["embedding"]))


# Commit the changes
db_connection.commit()

# Test data to use the copy_from method that is faster for very large datasets
test_data_copy = [
    {"title": "Article 4", "chunk": "This is the content of Article 4.",
        "embedding": [1, 2, 3]},
    {"title": "Article 5", "chunk": "This is the content of Article 5.",
        "embedding": [4, 5, 6]},
    {"title": "Article 6", "chunk": "This is the content of Article 6.",
        "embedding": [7, 8, 9]},
    # Add more test data as needed
]

# Create a StringIO object and write the test data to it
data_io = StringIO()

for data in test_data_copy:
    # Convert vector to string
    embedding_str = ','.join(map(str, data['embedding']))
    data_io.write(f"{data['title']}\t{data['chunk']}\t{embedding_str}\n")

cursor.execute("""
    CREATE TEMPORARY TABLE tmp_wikipedia AS
    SELECT * FROM wikipedia WHERE FALSE
""")
cursor.execute("""
    ALTER TABLE tmp_wikipedia
    ALTER COLUMN embedding TYPE TEXT
""")

data_io.seek(0)
cursor.copy_from(data_io, 'tmp_wikipedia', columns=(
    'title', 'chunk', 'embedding'), sep='\t')

cursor.execute("""
    INSERT INTO wikipedia (title, chunk, embedding)
    SELECT title, chunk, string_to_array(embedding, ',')::float[] FROM tmp_wikipedia
""")

# Commit the changes
db_connection.commit()

# Test data for execute_values
test_data_values = [
    {"title": "Article 7", "chunk": "This is the content of Article 7.",
        "embedding": [1, 2, 3]},
    {"title": "Article 8", "chunk": "This is the content of Article 8.",
        "embedding": [4, 5, 6]},
    {"title": "Article 9", "chunk": "This is the content of Article 9.",
        "embedding": [7, 8, 9]},
    # Add more test data as needed
]

# Formatting the data
formatted_data = [
    (data["title"], data["chunk"], data["embedding"])
    for data in test_data_values
]

# Create INSERT query
insert_query = """
    INSERT INTO wikipedia (title, chunk, embedding)
    VALUES %s
"""

# Using psycopg2.extras.execute_values() to insert the list of tuples
psycopg2.extras.execute_values(cursor, insert_query, formatted_data)

# Commit the changes
db_connection.commit()

# Close the cursor and the connection
cursor.close()
db_connection.close()
