import configparser
import pandas as pd
import pathlib
import psycopg2
import sys
import matplotlib.pyplot as plt
from psycopg2 import sql

def query_redshift_data():
    # Parse configuration
    script_path = pathlib.Path(__file__).parent.resolve()
    parser = configparser.ConfigParser()
    parser.read(f"{script_path}/configuration.conf")
    
    # Connection details
    USERNAME = parser.get("aws_config", "redshift_username")
    PASSWORD = parser.get("aws_config", "redshift_password")
    HOST = parser.get("aws_config", "redshift_hostname")
    PORT = parser.get("aws_config", "redshift_port")
    DATABASE = parser.get("aws_config", "redshift_database")
    
    # Connect to Redshift
    conn = psycopg2.connect(
        dbname=DATABASE, 
        user=USERNAME, 
        password=PASSWORD, 
        host=HOST, 
        port=PORT
    )
    
    # Example 1: Get top posts by score
    query1 = """
    SELECT *
    FROM reddit
    ORDER BY score DESC
    LIMIT 2
    """
    
    top_posts_df = pd.read_sql(query1, conn)
    print("\n=== TOP POSTS BY SCORE ===")
    print(top_posts_df)
    
    # Example 2: Analysis by subreddit
    query2 = """
    SELECT 
        COUNT(*) as post_count,
        AVG(score) as avg_score,
        AVG(num_comments) as avg_comments
    FROM reddit
    GROUP BY subreddit
    HAVING COUNT(*) > 5
    ORDER BY avg_score DESC
    """
    
    subreddit_stats_df = pd.read_sql(query2, conn)
    print("\n=== SUBREDDIT STATISTICS ===")
    print(subreddit_stats_df)
    
    # Example 3: Time-based analysis 
    query3 = """
    SELECT 
        EXTRACT(HOUR FROM created_utc) as hour_of_day,
        AVG(score) as avg_score
    FROM reddit
    GROUP BY hour_of_day
    ORDER BY hour_of_day
    """
    
    time_analysis_df = pd.read_sql(query3, conn)
    
    # Optional: Create a visualization
    plt.figure(figsize=(10, 6))
    plt.bar(time_analysis_df['hour_of_day'], time_analysis_df['avg_score'])
    plt.title('Average Post Score by Hour of Day')
    plt.xlabel('Hour of Day')
    plt.ylabel('Average Score')
    plt.xticks(range(0, 24))
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig('score_by_hour.png')
    
    # Close connection
    conn.close()
    
    print("\nAnalysis complete! Check 'score_by_hour.png' for visualization.")

if __name__ == "__main__":
    query_redshift_data()