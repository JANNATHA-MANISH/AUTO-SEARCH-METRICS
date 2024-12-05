import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
import json

# Load environment variables from .env file
load_dotenv()

# Retrieve Supabase and database credentials
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

# Verify environment variables are loaded
if not SUPABASE_URL or not SUPABASE_API_KEY or not DATABASE_URL:
    raise ValueError("Required environment variables are missing in the .env file.")

# Connect to the Supabase/PostgreSQL database
def connect_db():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

# Close the database connection
def close_db(conn, cursor):
    cursor.close()
    conn.close()

# Function to calculate average CTR
def calculate_average_ctr(cursor):
    cursor.execute("""
        SELECT search_date, 
               SUM(clicks) / NULLIF(SUM(impressions), 0) AS average_ctr
        FROM search_clicks
        GROUP BY search_date
        ORDER BY search_date;
    """)
    return cursor.fetchall()


# Get top 5 search queries with the highest CTR
def get_top_queries(cursor):
    cursor.execute("""
        SELECT search_query, 
               SUM(clicks) / NULLIF(SUM(impressions), 0) AS ctr
        FROM search_clicks
        GROUP BY search_query
        ORDER BY ctr DESC
        LIMIT 5;
    """)
    return cursor.fetchall()

# Get low-performance queries
def get_low_performance_queries(cursor):
    cursor.execute("""
        SELECT search_query
        FROM search_clicks
        WHERE impressions > 1000 AND clicks < 50
        ORDER BY impressions DESC;
    """)
    return cursor.fetchall()

# Insert insights into `search_insights` table
def insert_insights(cursor, conn, average_ctr, top_queries, low_performance_queries):
    insight_data = {
        "insight_date": datetime.now().strftime('%Y-%m-%d'),
        "average_ctr": average_ctr,
        "top_queries": json.dumps(top_queries),
        "low_performance_queries": json.dumps(low_performance_queries)
    }

    cursor.execute("""
        INSERT INTO search_insights (insight_date, average_ctr, top_queries, low_performance_queries)
        VALUES (%s, %s, %s, %s)
    """, (insight_data["insight_date"], 
          insight_data["average_ctr"], 
          insight_data["top_queries"], 
          insight_data["low_performance_queries"]))
    conn.commit()

# Run the pipeline
def run_pipeline():
    print("Starting the pipeline...")
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Calculate average CTR
        ctr_results = calculate_average_ctr(cursor)
        average_ctr = sum([result[1] for result in ctr_results]) / len(ctr_results) if ctr_results else 0
        print(f"Average CTR: {average_ctr}")

        # Get top queries with the highest CTR
        top_queries = get_top_queries(cursor)
        print(f"Top Queries: {top_queries}")

        # Get low-performance queries
        low_performance_queries = get_low_performance_queries(cursor)
        print(f"Low Performance Queries: {low_performance_queries}")

        # Insert insights into `search_insights` table
        insert_insights(cursor, conn, average_ctr, top_queries, low_performance_queries)
        print("Pipeline executed successfully! Insights saved.")
    except Exception as e:
        print(f"Error during pipeline execution: {e}")
    finally:
        close_db(conn, cursor)

# Scheduler function to run the pipeline daily
def schedule_pipeline():
    import schedule
    import time

    schedule.every().day.at("00:00").do(run_pipeline)  # Schedule the task to run daily at midnight

    print("Scheduler is running. Press Ctrl+C to exit.")
    while True:
        schedule.run_pending()
        time.sleep(1)

# Main execution
if __name__ == "__main__":
    choice = input("Choose an action: \n1. Run Pipeline Now\n2. Start Scheduler\nEnter your choice: ")

    if choice == "1":
        run_pipeline()
    elif choice == "2":
        schedule_pipeline()
    else:
        print("Invalid choice. Exiting.")
