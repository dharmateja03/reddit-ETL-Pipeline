import boto3
import csv
import io

def get_csv_headers(bucket_name, file_key):
    """Retrieve headers from a CSV file in S3"""
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Get just the first 1000 bytes of the file to read headers
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=file_key,
            Range='bytes=0-1000'
        )
        
        # Read the partial content
        content = response['Body'].read().decode('utf-8')
        
        # Parse the CSV content
        csv_reader = csv.reader(io.StringIO(content))
        headers = next(csv_reader)  # Get the first row (headers)
        
        print(f"CSV Headers ({len(headers)} columns):")
        for i, header in enumerate(headers):
            print(f"  {i+1}. {header}")
            
        return headers
    except Exception as e:
        print(f"Error getting CSV headers: {e}")
        return []

if __name__ == "__main__":
    # Update these values with your actual S3 details
    bucket_name = "etl-reddit"
    file_key = "20250320.csv"  # or "tmp/20250320.csv" if in a subfolder
    
    headers = get_csv_headers(bucket_name, file_key)
    
    # Output headers in a format you can copy-paste
    if headers:
        print("\nHeader list for your load script:")
        print(", ".join(headers))