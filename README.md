# Reddit Analytics Integration Platform

## Project Overview
This project implements a comprehensive data pipeline that extracts data from Reddit, processes it, and makes it available for analytics. The pipeline extracts posts from selected subreddits, loads them into AWS infrastructure, transforms the data using dbt, and visualizes insights using Power BI.

## Architecture
![![image](https://github.com/user-attachments/assets/d010e3a6-691b-482a-92f1-8bbaf9d277c6)
]

The data flows through the following stages:
1. **Extraction**: Python scripts connect to Reddit API to extract post data
2. **Storage**: Data is stored in AWS S3
3. **Loading**: Data is loaded into Amazon Redshift
4. **Transformation**: dbt models transform raw data into analytics-ready tables
5. **Visualization**: Power BI dashboards provide insights

## Technologies Used
- **Reddit API**: Source data extraction
- **Python**: Scripting language for extraction and loading
- **Apache Airflow**: Workflow orchestration
- **AWS S3**: Data lake storage
- **AWS Redshift**: Data warehouse
- **dbt (data build tool)**: Data transformation
- **Power BI**: Data visualization

## Setup Instructions

### Prerequisites
- Python 3.8+
- AWS account with S3 and Redshift access
- Reddit API credentials
- Apache Airflow
- dbt

### Configuration
1. Clone this repository
2. Create a `configuration.conf` file with your credentials:
```
[reddit_config]
client_id = YOUR_REDDIT_CLIENT_ID
secret = YOUR_REDDIT_SECRET

[aws_config]
aws_access_key_id = YOUR_AWS_ACCESS_KEY_ID
aws_secret_access_key = YOUR_AWS_SECRET_ACCESS_KEY
aws_region = us-east-2
bucket_name = YOUR_S3_BUCKET_NAME
redshift_username = YOUR_REDSHIFT_USERNAME
redshift_password = YOUR_REDSHIFT_PASSWORD
redshift_hostname = YOUR_REDSHIFT_HOSTNAME
redshift_port = 5439
redshift_database = dev
redshift_role = YOUR_REDSHIFT_ROLE
account_id = YOUR_AWS_ACCOUNT_ID
```

3. Install required Python packages:
```bash
pip install -r requirements.txt
```

### Running the Pipeline
The pipeline is orchestrated using Airflow DAGs:

1. **Extraction**: Pulls data from Reddit API
   ```bash
   python extract_reddit.py
   ```

2. **Upload to S3**: Stores CSV files in S3
   ```bash
   python upload_to_s3.py
   ```

3. **Load to Redshift**: Copies data from S3 to Redshift
   ```bash
   python load_to_redshift.py
   ```

4. **Transform with dbt**: Runs dbt models
   ```bash
   cd dbt_project && dbt run
   ```

Alternatively, run the entire pipeline using Airflow:
```bash
airflow dags trigger reddit_analytics_pipeline
```

## Data Models
The dbt transformation layer includes the following models:

- **Staging**: Raw Reddit post data with minimal cleanup
- **Intermediate**: Normalized and enhanced data with additional metrics
- **Marts**: Analytics-ready data organized by domain

## Dashboard
The Power BI dashboard provides insights including:
- Post engagement by subreddit
- Trending topics over time
- User activity patterns
- Content performance metrics

## Future Enhancements
- Sentiment analysis of post content
- Integration with additional social media platforms
- Predictive modeling for post popularity
- Real-time streaming pipeline

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments
- Reddit API for providing data access
- dbt community for transformation best practices
