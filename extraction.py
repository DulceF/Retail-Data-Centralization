import tabula
import os
import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from datetime import datetime
import psycopg2
import boto3
import sys
import requests
import json
from pandas import json_normalize
from io import StringIO


raw_file_path = r"E:\projects\multinational\db_creds_raw.yaml"
cleaned_file_path = r"E:\projects\multinational\db_creds_cleaned.yaml"

class DataExtractor:

    def read_rds_table(self, raw_file_path, table_name="legacy_users"):
        # Initialize the classes
        from utilis import DatabaseConnector
        connector = DatabaseConnector()

        # Connect to PostgreSQL server
        engine = connector.init_db_engine(raw_file_path)  # Get the engine from this function
        dbConnection = engine.connect()

        # Read data from PostgreSQL database table and load into a DataFrame instance
        df = pd.read_sql_table(table_name, dbConnection)
        dbConnection.close()

        print(type(df))
        return df

    def extract_from_s3(self, s3_path):
        bucket = "data-handling-public"
        key = "products.csv"

        # Connect to S3
        s3 = boto3.client("s3")

        # Download the file from S3
        file = s3.get_object(Bucket=bucket, Key=key)

        # Read CSV content into a DataFrame
        content = file["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(content))

        return df


if __name__ == "__main__":
    extractor = DataExtractor()

    #'USER DATA - Extract       
    #df_user = extractor.read_rds_table(raw_file_path, table_name= "legacy_users")
    #print(legacy_table.head())
    #print(df_user.shape)  # how many rows before cleaning?
    #print(df_user['join_date'].unique())  # see how many formats/values exist

    #'CARD DATA - Extract
    #df_card = extractor.read_rds_table(raw_file_path, table_name="dim_card_details")
    #print(df_card.shape) 
    #print(df_card)

    #'STORE DATA - Extract 
    #df_store = extractor.read_rds_table(raw_file_path, table_name="legacy_store_details")
    #print(df_store.shape)
    #print(df_store.columns) 
    #print(df_store)     
        
    s3_path = 's3://data-handling-public/products.csv'
    df_product = extractor.extract_from_s3(s3_path)
    print(df_product.columns)