import json

import pymysql
import pandas as pd
import boto3

# Get API keys
content = open('config.json')
config = json.load(content)
access_key = config['access_key']
secret_access_key = config['secret_access_key']
#MySQL Database Configuration
db_config = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "uscs_map_death",
}

#AWS S3 Configuration
AWS_ACCESS_KEY = access_key
AWS_SECRET_KEY = secret_access_key
S3_BUCKET_NAME = "my-unique-bucket-20041234"
S3_FILE_NAME = "exported_data.csv"  # Name of file in S3

#Local CSV file path
local_csv_file = "C:/Users/flori/PycharmProjects/SqltoS3bucket/exported_data.csv"

try:
    # Connect to MySQL
    connection = pymysql.connect(**db_config)
    query = "SELECT * FROM uscs_map_death.uscs_map_death_all;"

#Load data into a Pandas DataFrame
    df = pd.read_sql(query, connection)

#Save DataFrame to CSV
    df.to_csv(local_csv_file, index=False)

    print("Data exported to CSV successfully.")

     #Upload to AWS S3
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name="eu-central-1"
    )

    s3_client.upload_file(local_csv_file, S3_BUCKET_NAME, S3_FILE_NAME)

    print(f"File uploaded to S3: s3://{S3_BUCKET_NAME}/{S3_FILE_NAME}")

except Exception as e:
    print(f"Error: {e}")

finally:
    if connection:
        connection.close()