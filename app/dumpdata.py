import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Retrieve Supabase database URL from the environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

# Function to connect to the PostgreSQL database
def connect_db():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

# Function to insert dummy data into the search_clicks table
def insert_dummy_data():
    conn = connect_db()
    cursor = conn.cursor()

    # Generate dummy data (CTR is not explicitly provided, it will be auto-calculated)
    data = [
        ('FastAPI tutorial', 120, 2000, '2024-12-01'),
        ('Hushh ai', 900, 2000, '2024-12-01'),
        ('PostgreSQL vs MySQL', 250, 3000, '2024-12-02'),
        ('What is Python?', 50, 1000, '2024-12-03'),
        ('How to use Supabase?', 180, 2000, '2024-12-04'),
        ('Learn SQL', 85, 1600, '2024-12-05'),
        ('MjaiChat Platform', 350, 1000, '2024-12-06'),
        ('Company Hussh.ai', 500, 1200, '2024-12-07'),
        ('Optimize database', 10, 900, '2024-12-08'),
        ('Data Analysis', 220, 2500, '2024-12-09'),
        ('React vs Angular', 180, 1500, '2024-12-10'),
        ('Python Flask', 150, 700, '2024-12-11'),
        ('FastAPI vs Django', 110, 1400, '2024-12-12'),
        ('Web Development', 135, 1200, '2024-12-13'),
        ('API Security', 90, 1000, '2024-12-14'),
        ('PostgreSQL tips', 60, 1100, '2024-12-15'),
        ('Machine Learning', 75, 1300, '2024-12-16'),
        ('Mjaichat portfolio', 600, 1200, '2024-12-17'),
        ('Cloud Computing', 120, 2000, '2024-12-18'),
        ('Data Science', 180, 1800, '2024-12-19'),
        ('How to create a website?', 5, 5000, '2024-12-01'),
        ('Advanced Data Structures', 3, 4000, '2024-12-02'),
        ('Learn Angular', 2, 3500, '2024-12-03'),
        ('Big Data Analysis', 8, 6000, '2024-12-04'),
        ('Machine Learning Tips', 1, 5000, '2024-12-05'),
        ('SEO Best Practices', 4, 4500, '2024-12-06'),
        ('Learn Java', 6, 6000, '2024-12-07'),
        ('Python for Data Science', 3, 5000, '2024-12-08')
    ]

    # Inserting the dummy data into the search_clicks table (without specifying click_through_rate)
    cursor.executemany("""
        INSERT INTO search_clicks (search_query, clicks, impressions, search_date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (search_query, search_date) 
        DO UPDATE SET clicks = EXCLUDED.clicks, impressions = EXCLUDED.impressions
    """, data)

    conn.commit()  # Commit the transaction
    cursor.close()
    conn.close()
    print("Dummy data inserted successfully!")

# Call the function to insert dummy data
if __name__ == "__main__":
    insert_dummy_data()
