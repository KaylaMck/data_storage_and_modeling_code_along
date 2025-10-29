import boto3
import pandas as pd
import io
from sqlalchemy import create_engine

MINIO_URL = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw"

DB_USER = "myuser"
DB_PASSWORD = "mypassword"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "de"

def get_db_engine():

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return engine

def get_s3_client():

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    return s3_client

def process_customers(s3, engine):

    file_name = "customers.csv"
    response = s3.get_object(Bucket=BUCKET_NAME, Key=file_name)
    file_content = response['Body'].read()
    customers = pd.read_csv(io.BytesIO(file_content))
    table_name = file_name.split(".")[0]
    customers.to_sql(table_name, engine, if_exists="replace", index=False)

def process_products(s3, engine):

    file_name = "products.json"
    response = s3.get_object(Bucket=BUCKET_NAME, Key=file_name)
    file_content = response['Body'].read()
    products = pd.read_json(io.BytesIO(file_content))
    table_name = file_name.split(".")[0]
    products.to_sql(table_name, engine, if_exists="replace", index=False)

def process_sales(s3, engine):

    file_name = "sales.parquet"
    response = s3.get_object(Bucket=BUCKET_NAME, Key=file_name)
    file_content = response['Body'].read()
    sales = pd.read_parquet(io.BytesIO(file_content))
    table_name = file_name.split(".")[0]
    sales.to_sql(table_name, engine, if_exists="replace", index=False)

def main():

    s3 = get_s3_client()
    engine = get_db_engine()

    process_customers(s3, engine)
    process_products(s3, engine)
    process_sales(s3, engine)

if __name__ == "__main__":
    main()