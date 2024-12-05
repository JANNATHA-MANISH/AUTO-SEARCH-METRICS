from db import connect_db, close_db
from metrics import calculate_average_ctr, get_top_queries, get_low_performance_queries
from metrics import insert_insights

def run_pipeline():
    conn = connect_db()
    cursor = conn.cursor()

    # Calculate metrics
    average_ctr_results = calculate_average_ctr(cursor)  # Get average CTR by day
    top_queries = get_top_queries(cursor)  # Get top queries
    low_performance_queries = get_low_performance_queries(cursor)  # Get low-performance queries

    # Display the results in a readable format
    """ print("Average CTR by Day:")
    for date, ctr in average_ctr_results:
        print(f"{date} - {ctr:.4f}") """
    
   # Calculate overall average CTR from the results
    if average_ctr_results:
        overall_average_ctr = sum(ctr for _, ctr in average_ctr_results) / len(average_ctr_results)
    else:
        overall_average_ctr = 0.0

    # Display the overall average CTR
    print(f"Overall Average CTR: {overall_average_ctr:.4f}")

    print("\nTop 5 Search Queries with Highest CTR:")
    # Display only the top 5 queries with highest CTR
    for idx, (query, ctr) in enumerate(top_queries[:5], start=1):
        print(f"Rank {idx}: Query: {query}, CTR: {ctr:.4f}")

    print("\nLow Performance Queries (High impressions but low clicks):")
    if low_performance_queries:
        for query in low_performance_queries:
            print(f"Query: {query[0]}")
    else:
        print("No low performance queries found.")

    # Save insights to the database
    insert_insights(cursor, conn, average_ctr_results, top_queries, low_performance_queries)

    close_db(conn, cursor)
    print("\nPipeline executed and data saved!")

if __name__ == "__main__":
    run_pipeline()
