import os
import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from datetime import datetime
import psycopg2
import tabula
file_path = r"D:\Aicore_projects\Data Manipulation\Multinational Retail Data\db_creds.yaml"

#(Milestone2, Task2, Step1)
#This class is used to extract data from different data sources
class DataExtractor:
        def __init__ (self): 
                pass

#(Milestone2, Task3, Step2)
##This will read the credentials of the yaml file and return a dictionary of the credentials
        def read_db_creds(self,file_path):
                with open(file_path, "r") as file:
                        data = yaml.safe_load(file)
                return data

#(Milestone2, Task3, Step3)
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

#(Milestone2, Task3, Step4)  
# Using the engine from init_db_engine, create a method to list all the tables in the database
        def list_db_tables(self, file_path):
                engine = self.init_db_engine(file_path)
                inspector = inspect(engine)
                tables = inspector.get_table_names()
                return tables

#(Milestone2, Task3, Step5) 
#In this step we will extract the database table to a pandas DataFrame  

        def read_rds_table(self, file_path, tables):
                #Connect to PostgreSQL server
                engine = self.init_db_engine(file_path) #Get the engine from this function
                dbConnection = engine.connect()
                #Read data from PostgreSQL database table and load into a DataFrame instance
                df = pd.read_sql_table("legacy_users", dbConnection)   
                return df

     
if __name__ == "__main__":
        file_path = r"D:\Aicore_projects\Data Manipulation\Multinational Retail Data\db_creds.yaml"
        extractor = DataExtractor()
        #credentials = read_db_creds(file_path)
        #engine = init_db_engine()
        tables = extractor.list_db_tables(file_path)

print(tables)



        


       

