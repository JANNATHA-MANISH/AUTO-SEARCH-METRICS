import psycopg2
from config import DATABASE_URL

# Function to connect to the Supabase/PostgreSQL database
def connect_db():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        print("Successfully connected to the database.")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

# Function to close the connection and cursor
def close_db(conn, cursor=None):
    try:
        if cursor:
            cursor.close()
        conn.close()
        print("Database connection closed.")
    except Exception as e:
        print(f"Error closing the database connection: {e}")

# Example function to query the database
def execute_query(query):
    conn = None
    cursor = None
    try:
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()  # Assuming you expect multiple rows
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
