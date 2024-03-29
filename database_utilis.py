from sqlalchemy import create_engine 
import psycopg2
import yaml
import pandas as pd
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


#(Milestone2, Task2, Step2)
#This class is used to connect and upload data to the database

class DatabaseConnector:
    
    def __init__(self, host, password, user,database,port):
        self.host = host
        self.password = password
        self.user = user
        self.database = database
        self.port = port
        
    def read_db_creds(self,file_path):
                with open(file_path, "r") as file:
                        data = yaml.safe_load(file)
                return data
    #Create a method to read credentials and initialise and return an sqlalchemy database engine
    def init_db_engine(self,file_path):
                credentials = self.read_db_creds(file_path)
                print(credentials)       
                #Extract the credentials
                host = credentials["host"]
                password = credentials["password"]
                user = credentials["user"]
                database = credentials["database"]
                port = credentials["port"]
                
                #Create and return the SQLAlchemy database engine
                engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")      
                return engine
    
    def upload_to_db(self,df, table_name,file_path):
        #file_path = r"D:\Aicore_projects\Data Manipulation\Multinational Retail Data\db_creds.yaml"
        extractor = DataExtractor()
        cleaner = DataCleaning()

        #Get the engine from the data_extraction.py script
        engine = extractor.init_db_engine(file_path)

        
        #'USER DATA' - After getting the engine, upload the DataFrame to the database (using Pandas 'to_sql') 
        
        table_name = "dim_users"
        df = cleaner.clean_user_data()
        df.to_sql("dim_users", engine, if_exists="replace", index= False)
        
        engine.dispose()
        
        #'CARD DETAILS'- After getting the engine, upload the DataFrame to the database (using Pandas 'to_sql')
        table_name = "dim_card_details"
        df = cleaner.clean_card_data()
        df.to_sql("dim_card_details", engine, if_exists="replace", index= False)
        
        engine.dispose()

if __name__ == "__main__":
    file_path = r"D:\Aicore_projects\Data Manipulation\Multinational Retail Data\db_creds.yaml"
    host= "data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
    password = "AiCore2022"
    user = "aicore_admin"
    database = "sales_data"
    port = "5432"
    table_name = "dim_users"

    extractor = DataExtractor()
    cleaner = DataCleaning()

    tables = extractor.list_db_tables(file_path)
    user_table_name = "legacy_users"
    df = extractor.read_rds_table(file_path, user_table_name)
    
    connection = extractor.init_db_engine(file_path)
    df_cleaned = cleaner.clean_user_data(df)       
            
    connector = DatabaseConnector(host, password, user,database,port)
    connector.upload_to_db(df, table_name,file_path)
        
