import io
import pandas as pd

BUCKET_NAME = "raw"

def process_customers(s3, engine):

    file_name = "customers.csv"
    response = s3.get_object(Bucket=BUCKET_NAME, Key=file_name)
    file_content = response["Body"].read()
    customers = pd.read_csv(io.BytesIO(file_content))
    table_name = file_name.split(".")[0]
    customers.to_sql(table_name, engine, if_exists="replace", index=False)


def process_products(s3, engine):

    file_name = "products.json"
    response = s3.get_object(Bucket=BUCKET_NAME, Key=file_name)
    file_content = response["Body"].read()
    products = pd.read_json(io.BytesIO(file_content))
    table_name = file_name.split(".")[0]
    products.to_sql(table_name, engine, if_exists="replace", index=False)


def process_sales(s3, engine):

    file_name = "sales.parquet"
    response = s3.get_object(Bucket=BUCKET_NAME, Key=file_name)
    file_content = response["Body"].read()
    sales = pd.read_parquet(io.BytesIO(file_content))
    table_name = file_name.split(".")[0]
    sales.to_sql(table_name, engine, if_exists="replace", index=False)