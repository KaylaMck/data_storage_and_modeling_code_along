import boto3
from sqlalchemy import create_engine
import process_files as pf
import my_logger as ml

MINIO_URL = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

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


def main(logger):
    logger.info("setting up s3 client and db engine")
    s3 = get_s3_client()
    engine = get_db_engine()

    logger.info("processing customer files")
    pf.process_customers(s3, engine)

    logger.info("processing product files")
    pf.process_products(s3, engine)

    logger.info("processing sales files")
    pf.process_sales(s3, engine)

    logger.info("all files processed successfully")


if __name__ == "__main__":

    logger = ml.get_my_logger()
    main(logger)