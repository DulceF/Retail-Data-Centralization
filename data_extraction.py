import tabula
import os
import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from datetime import datetime
import psycopg2
import requests
import json
from pandas import json_normalize


pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
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

        def read_rds_table(self, file_path, table_name):
                #Connect to PostgreSQL server
                engine = self.init_db_engine(file_path) #Get the engine from this function
                dbConnection = engine.connect()
                #Read data from PostgreSQL database table and load into a DataFrame instance
                df = pd.read_sql_table(table_name, dbConnection)   
                return df

        def retrieve_pdf_data(self,pdf_path):
                        #Read the PDF file and store the list of DataFrames in "tables". 
                        # Tabula returns a list of DataFrames,one for each page of the PDF that contains tabular data
                        pdf_tables = tabula.read_pdf(pdf_path, pages="all")
                        #Combine all Dataframes into a single DataFrame
                        combined_df = pd.concat(pdf_tables)
                        return  combined_df
                
                #               *** API's *** Milestine 2 Task 5

        #Step1
        def list_number_of_stores(self,list_url,headers):
                        #Send a GET request to Return the number of stores to extract
                        response = requests.get(list_url, headers= headers) 
                        #Check if the API request was successful
                        if response.status_code == 200:
                        #Parse the JSON response
                                data = response.json()
                                return data
                        else:
                                print(f"Request unsuccessful. Status code: {response.status_code}")
                                print(f"Response content: {response.text}")
                                return None
        #Step3
        def retrieve_stores_data(self,base_url,headers):
                        
                        store_info = []
                        response = requests.get(base_url,headers=headers)
                        
                                #Check if the API request was successful
                        if response.status_code == 200:
                                #Parse the JSON response
                                data = response.json()
                                #for store_data in data: 
                                        #Save the information
                                        #store_info.append(store_data)
                                store_info = json_normalize(data)
                        else:
                                print(f"request unsuccessful. Status code: {response.status_code}")
                                print(f"Response content: {response.text}")
                                
                        #Create a Pandas Dataframe                  
                        df = pd.DataFrame(store_info)
                        return df
                        

if  __name__ == "__main__":

                        file_path = r"D:\Aicore_projects\Data Manipulation\Multinational Retail Data\db_creds.yaml"        
                        extractor = DataExtractor()
                        #credentials = read_db_creds(file_path)
                        #engine = init_db_engine()
                        combined_df = extractor.retrieve_pdf_data(pdf_path)
                               
                        #Define the URL and headers for the API request
                        list_url =  "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
                        headers = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
                        data = extractor.list_number_of_stores(list_url,headers)
                        
                        
                        base_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
                        
                        #List containing store numbers
                        store_numbers = range(1,452)
                        #Loop over the store number and add them to the URL
                        data_frames = []
                        for store_number in store_numbers:
                                #print(f"This data is from store number: {store_number}")
                                #This is the url for the API request
                                url = f"{base_url}{store_number}" #Concatenate the url and the store number (join)
                                
                                #Make the API request 
                                stores_data = extractor.retrieve_stores_data(url,headers)
                                #print(f"Response data for store {store_number}:", stores_data)
                                
                                #Append the response to the data_frames list
                                data_frames.append(stores_data)
                                
                        #Concatenate all the dataframes into a single dataframe
                        if data_frames:
                                result_df = pd.concat(data_frames)       
                                #Save to Excel file
                                print(result_df)
                                result_df.to_excel("output_of_stores.xlsx", index = False)
                        else:
                                print("No data found")               
                                
                                


