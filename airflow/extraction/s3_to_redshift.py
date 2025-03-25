import configparser
import logging
import pathlib
import psycopg2
import sys
from psycopg2 import sql
from datetime import datetime

"""
Part of DAG. Upload S3 CSV data to Redshift. Takes one argument of format YYYYMMDD. This is the name of 
the file to copy from S3. Script will load data into temporary table in Redshift, delete 
records with the same post ID from main table, then insert these from temp table (along with new data) 
to main table. This means that if we somehow pick up duplicate records in a new DAG run,
the record in Redshift will be updated to reflect any changes in that record, if any (e.g. higher score or more comments).
"""

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('redshift_load.log')
    ]
)
logger = logging.getLogger('redshift_loader')

# Parse our configuration file
script_path = pathlib.Path(__file__).parent.resolve()
parser = configparser.ConfigParser()
config_path = f"{script_path}/configuration.conf"
logger.info(f"Reading configuration from {config_path}")
parser.read(config_path)

# Store our configuration variables
USERNAME = parser.get("aws_config", "redshift_username")
PASSWORD = parser.get("aws_config", "redshift_password")
HOST = parser.get("aws_config", "redshift_hostname")
PORT = parser.get("aws_config", "redshift_port")
REDSHIFT_ROLE = parser.get("aws_config", "redshift_role")
DATABASE = parser.get("aws_config", "redshift_database")
BUCKET_NAME = parser.get("aws_config", "bucket_name")
ACCOUNT_ID = parser.get("aws_config", "account_id")
TABLE_NAME = "reddit"

logger.info(f"Using Redshift host: {HOST}")
logger.info(f"Using S3 bucket: {BUCKET_NAME}")


def check_iam_role_permissions(rs_conn):
    """Verify the IAM role has proper permissions using a different method"""
    try:
        cur = rs_conn.cursor()
        logger.info(f"Checking IAM role permissions: {role_string}")
        
        # Test if role can access S3 directly
        try:
            bucket = file_path.replace('s3://', '').split('/')[0]
            key_prefix = '/'.join(file_path.replace('s3://', '').split('/')[1:-1])
            if not key_prefix.endswith('/'):
                key_prefix += '/'
                
            # Simplified test - just try to list objects
            test_query = f"""
            SELECT 'testing_s3_access';
            """
            cur.execute(test_query)
            logger.info("Basic query execution successful")
            
            # Now try a simple COPY to test S3 access
            cur.execute("DROP TABLE IF EXISTS test_s3_access; CREATE TEMP TABLE test_s3_access (col1 VARCHAR(100));")
            
            test_copy = f"""
            COPY test_s3_access FROM '{file_path}'
            iam_role '{role_string}'
            CSV IGNOREHEADER 1
            MAXERROR 1;
            """
            
            try:
                cur.execute(test_copy)
                logger.info("IAM role can access the S3 file")
                return True
            except Exception as e:
                logger.error(f"IAM role cannot access S3 file: {e}")
                # Check if this is a permissions issue or some other error
                if "Access Denied" in str(e) or "403" in str(e):
                    logger.error("This appears to be an S3 permissions issue")
                elif "does not exist" in str(e) or "404" in str(e):
                    logger.error("The S3 file appears not to exist")
                else:
                    logger.error("This appears to be a different kind of error")
                check_load_errors(rs_conn)
                return False
            finally:
                cur.execute("DROP TABLE IF EXISTS test_s3_access;")
                
        except Exception as e:
            logger.error(f"Error testing S3 access: {e}")
            return False
    except Exception as e:
        logger.error(f"Error checking IAM role: {e}")
        return False
   
    try:
        cur = rs_conn.cursor()
        logger.info(f"Checking IAM role permissions: {role_string}")
        # Test if role can be used by Redshift
        cur.execute(f"""
        SELECT count(*) 
        FROM svv_redshift_roles
        WHERE iam_role_arn = '{role_string}';
        """)
        role_count = cur.fetchone()[0]
        if role_count == 0:
            logger.error(f"IAM role {role_string} is not associated with Redshift")
        else:
            logger.info(f"IAM role {role_string} is properly associated with Redshift")
            
        # Test if role can access S3
        try:
            bucket = file_path.replace('s3://', '').split('/')[0]
            cur.execute(f"""
            SELECT aws_s3.list_objects(
                aws_commons.create_s3_uri('{bucket}', '', ''),
                '{role_string}'
            );
            """)
            logger.info("IAM role can access the S3 bucket")
            return True
        except Exception as e:
            logger.error(f"IAM role cannot access S3 bucket: {e}")
            return False
    except Exception as e:
        logger.error(f"Error checking IAM role: {e}")
        return False
# Check command line argument passed
try:
    output_name = sys.argv[1]
    logger.info(f"Using command line argument for filename: {output_name}")
except Exception as e:
    current_date = datetime.now().strftime('%Y%m%d')
    logger.info(f"Command line argument not provided, using current date: {current_date}")
    output_name = current_date

# Our S3 file & role_string
file_path = f"s3://{BUCKET_NAME}/{output_name}.csv"
role_string = f"arn:aws:iam::{ACCOUNT_ID}:role/{REDSHIFT_ROLE}"

logger.info(f"Will load data from: {file_path}")
logger.info(f"Using IAM role: {role_string}")

# Create Redshift table if it doesn't exist with expanded schema
"""
post_fields = [
            "id", "title", "score", "num_comments", "author", "created_utc",
            "url", "upvote_ratio", "over_18",  "spoiler", "stickied",
            "selftext", "subreddit"  # Added subreddit name and post content
        ]
"""
sql_create_table = sql.SQL(
    """DROP TABLE IF EXISTS reddit;
    CREATE TABLE IF NOT EXISTS {table} (
        id varchar(100) PRIMARY KEY,
        title varchar(4000),
        score int,
        num_comments int,
        author varchar(100),
        created_utc timestamp,
        url varchar(2000),
        upvote_ratio float,
        over_18 varchar(10),
        spoiler varchar(10),
        stickied varchar(10),
        selftext varchar(65535),
        subreddit varchar(100),
        extraction_timestamp timestamp,
        selftext_length int,
        is_nsfw varchar(10)
    );"""
).format(table=sql.Identifier(TABLE_NAME))

# If ID already exists in table, we remove it and add new ID record during load.
create_temp_table = """
CREATE TEMP TABLE our_staging_table (
    id varchar(100) PRIMARY KEY,
    title varchar(4000),
    score int,
    num_comments int,
    author varchar(100),
    created_utc timestamp,
    url varchar(2000),
    upvote_ratio float,
    over_18 varchar(10),
    spoiler varchar(10),
    stickied varchar(10),
    selftext varchar(65535),
    subreddit varchar(100),
    extraction_timestamp timestamp,
    selftext_length int,
    is_nsfw varchar(10)
);
"""

sql_copy_to_temp = f"""
COPY our_staging_table(id, title, score, num_comments, author, created_utc, url, 
                     upvote_ratio, over_18, spoiler, stickied, 
                     selftext, subreddit, extraction_timestamp,
                     selftext_length, is_nsfw)
FROM '{file_path}' 
iam_role '{role_string}' 
IGNOREHEADER 1 
DELIMITER ',' 
CSV 
ACCEPTINVCHARS AS ' '
EMPTYASNULL
TRUNCATECOLUMNS
MAXERROR 100
ACCEPTANYDATE
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
TRIMBLANKS
BLANKSASNULL;
"""

delete_from_table = sql.SQL(
    "DELETE FROM {table} USING our_staging_table WHERE {table}.id = our_staging_table.id;"
).format(table=sql.Identifier(TABLE_NAME))

insert_into_table = sql.SQL(
    "INSERT INTO {table} SELECT * FROM our_staging_table;"
).format(table=sql.Identifier(TABLE_NAME))

drop_temp_table = "DROP TABLE our_staging_table;"

def inspect_csv_structure(rs_conn):
    """Inspect the first few lines of the CSV to determine its structure"""
    try:
        cur = rs_conn.cursor()
        
        # Create a table to hold raw CSV lines
        cur.execute("DROP TABLE IF EXISTS csv_raw; CREATE TEMP TABLE csv_raw (line VARCHAR(65535));")
        
        # Load the first few lines without any parsing
        inspect_copy = f"""
        COPY csv_raw FROM '{file_path}'
        iam_role '{role_string}'
        DELIMITER '\n'
        MAXERROR 10
        LIMIT 5;
        """
        cur.execute(inspect_copy)
        
        # Look at what we got
        cur.execute("SELECT * FROM csv_raw ORDER BY line LIMIT 2;")
        lines = cur.fetchall()
        
        if lines:
            header = lines[0][0]
            logger.info(f"CSV header: {header}")
            columns = header.split(',')
            column_count = len(columns)
            logger.info(f"CSV has {column_count} columns: {columns}")
            
            # If there's a second line, examine a data row
            if len(lines) > 1:
                data_row = lines[1][0]
                logger.info(f"Sample data row: {data_row}")
                
            return columns
        else:
            logger.error("Could not load any lines from the CSV file")
            return []
            
    except Exception as e:
        logger.error(f"Error inspecting CSV structure: {e}")
        return []
    finally:
        try:
            cur.execute("DROP TABLE IF EXISTS csv_raw;")
        except:
            pass
def main():
    """Upload file form S3 to Redshift Table"""
    try:
        logger.info("Starting Redshift data load process")
        rs_conn = connect_to_redshift()
        # inspect_csv_structure(rs_conn)
        # check_iam_role_permissions(rs_conn)
        load_data_into_redshift(rs_conn)
        logger.info("Data load completed successfully")
    except Exception as e:
        logger.error(f"Data load process failed: {e}")
        sys.exit(1)


def connect_to_redshift():
    """Connect to Redshift instance"""
    try:
        logger.info(f"Connecting to Redshift at {HOST}:{PORT}")
        rs_conn = psycopg2.connect(
            dbname=DATABASE, user=USERNAME, password=PASSWORD, host=HOST, port=PORT
        )
        logger.info("Successfully connected to Redshift")
        return rs_conn
    
    except Exception as e:
        logger.error(f"Unable to connect to Redshift: {e}")
        sys.exit(1)


def check_load_errors(conn):
    """Check the load error details"""
    try:
        cur = conn.cursor()
        logger.info("Checking for load errors...")
        cur.execute("""
            SELECT * FROM sys_load_error_detail
            ORDER BY start_time DESC
            LIMIT 10
        """)
        errors = cur.fetchall()
        if errors:
            logger.error("=== LOAD ERRORS ===")
            for error in errors:
                logger.error(f"Error: {error}")
            logger.error("===================")
        else:
            logger.info("No recent load errors found")
    except Exception as e:
        logger.error(f"Error checking load errors: {e}")


def load_data_into_redshift(rs_conn):
    """Load data from S3 into Redshift"""
    try:
        with rs_conn:
            cur = rs_conn.cursor()
            
            # Create main table if not exists
            logger.info("Creating or verifying main table structure")
            cur.execute(sql_create_table)
            
            # Create temporary staging table
            logger.info("Creating temporary staging table")
            cur.execute(create_temp_table)
            
            # Copy data from S3 to staging table
            logger.info(f"Copying data from {file_path} to staging table")
            cur.execute(sql_copy_to_temp)
            
            # Get staging table row count
            cur.execute("SELECT COUNT(*) FROM our_staging_table")
            staging_count = cur.fetchone()[0]
            logger.info(f"Loaded {staging_count} rows into staging table")
            
            # Delete existing records with same IDs
            logger.info("Removing existing records with same IDs from main table")
            cur.execute(delete_from_table)
            
            # Insert new/updated records
            logger.info("Inserting records from staging table to main table")
            cur.execute(insert_into_table)
            
            # Get main table count after insert
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {table}").format(table=sql.Identifier(TABLE_NAME)))
            final_count = cur.fetchone()[0]
            logger.info(f"Main table now has {final_count} rows")
            
            # Drop staging table
            logger.info("Dropping staging table")
            cur.execute(drop_temp_table)
            
            # Commit transaction
            rs_conn.commit()
            logger.info("Transaction committed successfully")
            
    except Exception as e:
        logger.error(f"Error loading data into Redshift: {e}")
        check_load_errors(rs_conn)
        raise


if __name__ == "__main__":
    main()