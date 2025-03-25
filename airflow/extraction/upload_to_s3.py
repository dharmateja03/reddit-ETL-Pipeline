import boto3
import botocore
import configparser
import pathlib
import sys
import os
import logging
from datetime import datetime

"""
Part of DAG. Take Reddit data and upload to S3 bucket.
Takes one command line argument of format YYYYMMDD.
This represents the file downloaded from Reddit.
"""

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('reddit_extractor')

# Load AWS credentials

parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_path = f"{script_path}/configuration.conf"


if not os.path.exists(config_path):
    print(f"Configuration file not found at {config_path}")
    sys.exit(1)

parser.read(config_path)
BUCKET_NAME = parser.get("aws_config", "bucket_name")
AWS_REGION = parser.get("aws_config", "aws_region")
aws_access_key_id = parser.get("aws_config", "aws_access_key_id")
aws_secret_access_key = parser.get("aws_config", "aws_secret_access_key")
# Get filename from command line or use default

current_date = datetime.now().strftime('%Y%m%d')
print("Command line argument not provided, using current date '{current_date}'")
output_name = current_date

# Name for our S3 file
FILENAME = f"{output_name}.csv"
KEY = FILENAME

def main():
    print("in main")
    """Upload input file to S3 bucket"""
    try:
        # Source file path - adjust this to where your file is actually located
        source_file_path = f"/Users/dharmatejasamudrala/reddit-etl/tmp/{output_name}.csv"
        
        if  os.path.exists(source_file_path):
            print("file path okay")

        
        # Check if file exists
        if not os.path.exists(source_file_path):
            print(f"Error: File not found at {source_file_path}")
            sys.exit(1)
            
        conn = connect_to_s3()
        create_bucket_if_not_exists(conn , source_file_path)
        upload_file_to_s3(conn, source_file_path)
        print(f"Successfully uploaded {source_file_path} to s3://{BUCKET_NAME}/{KEY}")
    except Exception as e:
        print(f"An error occurred at: {e}")
        sys.exit(1)

def connect_to_s3():
    """Connect to S3 Instance"""
    try:
        # Include region in the connection
        conn = boto3.resource("s3", aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,region_name=AWS_REGION)
        logger.info("Sucessfully connected to S3")
        return conn
    except Exception as e:
        print(f"Can't connect to S3. Error: {e}")
        sys.exit(1)

def create_bucket_if_not_exists(conn, file_path):
    """Check if bucket exists and create if not"""
    try:
        conn.meta.client.upload_file(Filename=file_path,Key=KEY,Bucket=BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} already exists")
    except botocore.exceptions.ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            print(f"Creating bucket {BUCKET_NAME} in region {AWS_REGION}")
            try:
                # Different creation method for us-east-1
                if AWS_REGION == "us-east-1":
                    conn.create_bucket(Bucket=BUCKET_NAME)
                else:
                    conn.create_bucket(
                        Bucket=BUCKET_NAME,
                        CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
                    )
                print(f"Bucket {BUCKET_NAME} created successfully")
            except Exception as e:
                print(f"Error creating bucket: {e}")
                sys.exit(1)
        else:
            print(f"Error checking bucket: {e}")
            sys.exit(1)
    logger.info("Sucessfully created bucket ")

def upload_file_to_s3(conn, file_path):
    """Upload file to S3 Bucket"""
    try:
        conn.meta.client.upload_file(
            Filename=file_path, Bucket=BUCKET_NAME, Key=KEY
        )
        logger.info("Sucessfully Uploaded to S3")
    except botocore.exceptions.ClientError as e:
        print(f"Error uploading file to S3: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()