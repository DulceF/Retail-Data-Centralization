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
#Handles extraction of data from multiple sources.
    
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
        product_df = pd.read_csv(StringIO(content))

        return product_df


if __name__ == "__main__":
    extractor = DataExtractor()

    #Extract USER DATA        
    #df_user = extractor.read_rds_table(raw_file_path, table_name= "legacy_users")
    #print(df_user)
    

    #Extract CARD DATA 
    #df_card = extractor.read_rds_table(raw_file_path, table_name="dim_card_details") 
    #print(df_card)

    #Extract STORE DATA 
    #df_store = extractor.read_rds_table(raw_file_path, table_name="legacy_store_details")
    #print(df_store)   
    
    #Extract ORDERS_DATA 
    df_orders = extractor.read_rds_table(raw_file_path, table_name="orders_table")
    print(df_orders.columns)
        
    #s3_path = 's3://data-handling-public/products.csv'
    #product_df = extractor.extract_from_s3(s3_path)
    #print(product_df.columns)