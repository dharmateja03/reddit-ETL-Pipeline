import configparser
import datetime
import logging
import pathlib
import time
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
import praw
import numpy as np
from praw.exceptions import PRAWException, RedditAPIException

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('reddit_extractor')

# Read Configuration File
def get_config():
    parser = configparser.ConfigParser()
    script_path = pathlib.Path(__file__).parent.resolve()
    config_file = "configuration.conf"
    config_path = f"{script_path}/{config_file}"
    
    if not pathlib.Path(config_path).exists():
        logger.error(f"Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    parser.read(config_path)
    return parser

# Reddit API connection
def api_connect(client_id: str, secret: str, user_agent: str = "Data Pipeline/1.0") -> praw.Reddit:
    """Connect to Reddit API with retry logic"""
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Connecting to Reddit API (attempt {attempt+1}/{max_retries})")
            instance = praw.Reddit(
                client_id=client_id, 
                client_secret=secret, 
                user_agent=user_agent
            )
            # Verify connection works by checking read-only status
            instance.read_only
            logger.info("Successfully connected to Reddit API")
            return instance
        except (PRAWException, RedditAPIException) as e:
            logger.warning(f"API connection failed: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Maximum retries reached. Failed to connect to Reddit API.")
                raise

def subreddit_posts(
    reddit_instance: praw.Reddit, 
    subreddit_name: str, 
    time_filter: str = "day", 
    limit: Optional[int] = None
):
    """Create posts object for Reddit instance with error handling"""
    try:
        logger.info(f"Fetching posts from r/{subreddit_name} (time filter: {time_filter})")
        subreddit = reddit_instance.subreddit(subreddit_name)
        posts = subreddit.top(time_filter=time_filter, limit=limit)
        return posts
    except (PRAWException, RedditAPIException) as e:
        logger.error(f"Failed to fetch posts: {e}")
        raise

def extract_data(posts, post_fields: List[str]) -> pd.DataFrame:
    """Extract Data to Pandas DataFrame with data validation"""
    list_of_items = []
    count = 0
    
    try:
        logger.info("Extracting post data")
        for submission in posts:
            # Add throttling to avoid rate limits
            if count % 100 == 0 and count > 0:
                logger.info(f"Processed {count} posts so far")
                time.sleep(1)
            
            to_dict = vars(submission)
            sub_dict = {field: to_dict.get(field) for field in post_fields}
            
            # Convert timestamp to datetime
            if 'created_utc' in sub_dict:
                sub_dict['created_utc'] = datetime.fromtimestamp(sub_dict['created_utc'])
            
            # Convert author to string to handle deleted accounts
            if 'author' in sub_dict and sub_dict['author'] is not None:
                sub_dict['author'] = str(sub_dict['author'])
            
            list_of_items.append(sub_dict)
            count += 1
        
        logger.info(f"Finished processing {count} posts")
        
        if not list_of_items:
            logger.warning("No posts were extracted")
            return pd.DataFrame(columns=post_fields)
        
        extracted_data_df = pd.DataFrame(list_of_items)
        text_columns = ['title', 'selftext']
        
        
        # Add extraction timestamp
        extracted_data_df['extraction_timestamp'] = datetime.now()
        
        # Data validation checks
        logger.info("Validating extracted data")
        missing_values = extracted_data_df.isna().sum().sum()
        logger.info(f"Dataset contains {missing_values} missing values")
        
        return extracted_data_df
    
    except Exception as e:
        logger.error(f"Data extraction failed: {e}")
        raise

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply transformations to the extracted data"""
    logger.info("Transforming Reddit data")
    
    try:
        # Create a copy to avoid modifying the original
        transformed_df = df.copy()
        if 'selftext' in transformed_df.columns:
            transformed_df['selftext'] = transformed_df['selftext'].fillna('')
            transformed_df['selftext_length'] = transformed_df['selftext'].str.len().astype(int)
        
        # Convert dates to proper datetime if not already
        if 'created_utc' in transformed_df.columns:
            transformed_df['created_utc'] = pd.to_datetime(transformed_df['created_utc'])
            
            # # Extract date components
            # transformed_df['date'] = transformed_df['created_utc'].dt.r
            # transformed_df['year'] = transformed_df['created_utc'].dt.year
            # transformed_df['month'] = transformed_df['created_utc'].dt.month
            # transformed_df['day'] = transformed_df['created_utc'].dt.day
            # transformed_df['day_of_week'] = transformed_df['created_utc'].dt.dayofweek
            # transformed_df['hour'] = transformed_df['created_utc'].dt.hour
        
        
        
        # Calculate engagement metrics
        # if all(col in transformed_df.columns for col in ['score', 'num_comments']):
            # transformed_df['engagement_score'] = transformed_df['score'] + transformed_df['num_comments'] * 2
            
            # # Categorize posts by popularity
            # transformed_df['popularity_category'] = pd.cut(
            #     transformed_df['engagement_score'],
            #     bins=[0, 10, 50, 100, float('inf')],
            #     labels=['Low', 'Medium', 'High', 'Viral']
            # )
        
        # Flag potential NSFW content
        if 'over_18' in transformed_df.columns:
            transformed_df['is_nsfw'] = transformed_df['over_18']
        
        # Handle missing values
        numeric_columns = ['score', 'num_comments', 'upvote_ratio']
        for col in numeric_columns:
            if col in transformed_df.columns:
                transformed_df[col] = transformed_df[col].fillna(0)
        
        logger.info(f"Transformation complete. Dataframe shape: {transformed_df.shape}")
        return transformed_df
        
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise

def save_to_csv(df: pd.DataFrame, output_path: str = None) -> str:
    """Save the dataframe to a CSV file"""
    if output_path is None:
        # Generate a default filename with timestamp
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f"reddit_data_{timestamp}.csv"
    
    try:
        logger.info(f"Saving data to {output_path}")
        
        df.to_csv(output_path, index=False)
        
        logger.info(f"Successfully saved data to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Failed to save data: {e}")
        raise

def main(subreddit_name: str = "stocks", time_filter: str = "day", limit: Optional[int] = None, output_path: str = None):
    """Extract Reddit data, transform, and save to CSV"""
    try:
        # Get configuration
        config = get_config()
        secret = config.get("reddit_config", "secret") 
        client_id = config.get("reddit_config", "client_id")
        
        # Define fields to extract
        post_fields = [
            "id", "title", "score", "num_comments", "author", "created_utc",
            "url", "upvote_ratio", "over_18",  "spoiler", "stickied",
            "selftext", "subreddit"  # Added subreddit name and post content
        ]
        
        # Connect to Reddit API
        reddit_instance = api_connect(client_id, secret)
        
        # Get subreddit posts
        posts = subreddit_posts(reddit_instance, subreddit_name, time_filter, limit)
        
        # Extract data
        raw_data = extract_data(posts, post_fields)
        
        # Transform data
        transformed_data = transform_data(raw_data)
        
        # Print summary statistics
        logger.info(f"Extracted and transformed {len(transformed_data)} posts from r/{subreddit_name}")
        
        # Display sample and stats
        logger.info(f"Sample data:\n{transformed_data.head(3)}")
        head=list(transformed_data.iloc[0])
        # for i in head:
        #     print(i,type(i),"\n")
        
        if not transformed_data.empty:
            # Generate basic statistics
            if 'score' in transformed_data.columns:
                avg_score = transformed_data['score'].mean()
                max_score = transformed_data['score'].max()
                logger.info(f"Average score: {avg_score:.2f}, Max score: {max_score}")
            
            if 'num_comments' in transformed_data.columns:
                avg_comments = transformed_data['num_comments'].mean()
                max_comments = transformed_data['num_comments'].max()
                logger.info(f"Average comments: {avg_comments:.2f}, Max comments: {max_comments}")
            
            # Save to CSV if requested
            if output_path:
                save_to_csv(transformed_data, output_path)
        
        return transformed_data
    
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    # You can modify these parameters or add command line arguments
    current_date = datetime.now().strftime('%Y%m%d')
    output_name = current_date
    main(subreddit_name="stocks", time_filter="week", limit=1000, output_path=f"/Users/dharmatejasamudrala/reddit-etl/tmp/{output_name}.csv")
