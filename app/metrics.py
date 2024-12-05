import json
from datetime import datetime

# Function to calculate the average CTR by day
def calculate_average_ctr(cursor):
    cursor.execute("""
        SELECT search_date, 
               ROUND(SUM(clicks)::NUMERIC / NULLIF(SUM(impressions), 0), 4) AS average_ctr
        FROM search_clicks
        GROUP BY search_date
        ORDER BY search_date;
    """)
    results = cursor.fetchall()
    # Returning as a list of tuples (date, average_ctr)
    return results

# Function to get the top 5 search queries with the highest CTR
def get_top_queries(cursor):
    cursor.execute("""
        SELECT search_query, 
               ROUND(SUM(clicks)::NUMERIC / NULLIF(SUM(impressions), 0), 4) AS ctr
        FROM search_clicks
        GROUP BY search_query
        ORDER BY ctr DESC
        LIMIT 5;
    """)
    results = cursor.fetchall()
    # Returning the top queries and their CTRs
    return results

# Function to get low performance queries (high impressions but low clicks)
def get_low_performance_queries(cursor):
    cursor.execute("""
        SELECT search_query
        FROM search_clicks
        WHERE impressions > 1000 AND clicks < 50
        ORDER BY impressions DESC;
    """)
    results = cursor.fetchall()
    # Returning low-performance queries
    return results

# Function to insert results into `search_insights` table
# Function to insert results into `search_insights` table
def insert_insights(cursor, conn, average_ctr, top_queries, low_performance_queries):
    # Convert Decimal to float
    average_ctr_value = sum([float(ctr) for date, ctr in average_ctr]) / len(average_ctr) if average_ctr else 0
    
    # Convert the results to a format suitable for JSON serialization
    top_queries = [(query, float(ctr)) for query, ctr in top_queries]
    low_performance_queries = [query[0] for query in low_performance_queries]

    # Prepare the data to be inserted
    insight_data = {
        "insight_date": datetime.now().strftime('%Y-%m-%d'),
        "average_ctr": average_ctr_value,
        "top_queries": json.dumps(top_queries),  # Now this will be serializable
        "low_performance_queries": json.dumps(low_performance_queries)
    }

    # Insert the insights into the search_insights table
    cursor.execute("""
        INSERT INTO search_insights (insight_date, average_ctr, top_queries, low_performance_queries)
        VALUES (%s, %s, %s, %s)
    """, (insight_data["insight_date"], 
          insight_data["average_ctr"], 
          insight_data["top_queries"], 
          insight_data["low_performance_queries"]))
    conn.commit()