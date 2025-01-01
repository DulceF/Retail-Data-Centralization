from sqlalchemy import create_engine 
import psycopg2
import yaml
import pandas as pd
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

extractor = DataExtractor()
cleaner = DataCleaning()


file_path = r"E:\\Aicore projects\Multinational_data\\db_creds.yaml"
dest_file_path = r"E:\\Aicore projects\Multinational_data\\postgres_creds.yaml"

#(Milestone2, Task2, Step2)
#This class is used to connect and upload data to the database

class DatabaseConnector:  
     
    def __init__(self):
        pass

    #1 Reads and returns database credentials from a YAML file.
    def read_db_creds(self,file_path):
                with open(file_path, "r") as file:
                        credentials = yaml.safe_load(file)
                        print(credentials)
                return credentials
    #2  Initialises and returns an sqlalchemy database engine
    def init_db_engine(self,file_path):
                credentials = self.read_db_creds(file_path)
                     
                #print(f"Connecting to: postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
                connection_str = f"postgresql+psycopg2://{credentials['username']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['database']}"
                
                #Create and return the SQLAlchemy database engine
                engine = create_engine(connection_str)      
                return engine
    
    #3 Uploads cleaned DataFrames to database tables
    def upload_to_db(self,df, table_name,dest_file_path):
       
       #Get the engine from the data_extraction.py script
        engine = self.init_db_engine(dest_file_path)
       
        try:
            #Upload the cleaned dataframe to its specific table in the database
            df.to_sql(table_name,engine, if_exists="replace", index=False)
            print(f"Successfully uploaded '{table_name}' to the database")     
        except Exception as e:
           print(f"Error uploading table '{table_name}': {str(e)}")   
        finally: 
            engine.dispose()


                     
if __name__ == "__main__":
    file_path = r"E:\\Aicore projects\Multinational_data\\db_creds.yaml"
    dest_file_path = r"E:\\Aicore projects\Multinational_data\\postgres_creds.yaml"
    pdf_path = r"https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    base_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    headers = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    store_numbers = range(1,451)
    
    #Initialize the classes
    extractor = DataExtractor()
    cleaner = DataCleaning()
    connector = DatabaseConnector()
    
    
    #'USER DATA' -    #Upload Dataframe to table "dim_users" in the database                    
    df_user = cleaner.clean_user_data(file_path)          
    connector.upload_to_db(df_user, "dim_users", dest_file_path) 
    print("Sucessfuly uploaded 'dim_users' to the database")

    #'CARD DETAILS' - #Upload Dataframe to table table "dim_card_details" in the database
    df_card = cleaner.clean_card_data(pdf_path)
    connector.upload_to_db(df_card, "dim_card_details", dest_file_path) 
    print("Sucessfuly uploaded 'dim_users' to the database")

    #'STORE DETAILS' - #Upload Dataframe to table 'dim_store_details' in the database
    df_store = cleaner.called_clean_store_data(base_url,headers,store_numbers)
    connector.upload_to_db(df_store, "dim_store_details", dest_file_path) 

          
    #2 Clean the extracted data
    #connection = extractor.init_db_engine(file_path)
    #df_cleaned = cleaner.clean_user_data(file_path)       
  
  #Table names: ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']  

