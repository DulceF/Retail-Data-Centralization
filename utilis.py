from sqlalchemy import create_engine 
from sqlalchemy import inspect
import psycopg2
import yaml
import pandas as pd

raw_file_path = r"E:\projects\multinational\db_creds_raw.yaml"
cleaned_file_path = r"E:\projects\multinational\db_creds_cleaned.yaml"

class DatabaseConnector:
    
    #def __init__(self, extractor,cleaner):
        #self.extractor = extractor
        #self.cleaner = cleaner 
    
    #1 Reads and returns database credentials from a YAML file.
    def read_db_creds(self, raw_file_path):
        with open(raw_file_path, "r") as file:
            credentials = yaml.safe_load(file)
            print(credentials)
        return credentials

    #2 Initialise and return an sqlalchemy engine
    def init_db_engine(self, raw_file_path):
        credentials = self.read_db_creds(raw_file_path)
                     
        #print(f"Connecting to: postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
        connection_str = f"postgresql+psycopg2://{credentials['username']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['database']}"
                
        #Create and return the SQLAlchemy database engine
        engine = create_engine(connection_str)      
        return engine

    #3 List the tables in the database
    def list_db_tables(self, raw_file_path):
        engine = self.init_db_engine(raw_file_path)
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables
    
    #4 Upload the cleaned table to the database(user df) 
    def upload_to_db(self,df, table_name, cleaned_file_path):
         
       #Get the engine from the data_extraction.py script and connect to the cleaned database
        engine = self.init_db_engine(cleaned_file_path)
       
        try:
            #Upload the cleaned dataframe to its specific table in the database
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            print(f"Successfully uploaded '{table_name}' to the database")     
        except Exception as e:
           print(f"Error uploading table '{table_name}': {str(e)}")   
        finally: 
            engine.dispose()


if __name__ == "__main__":    
#Initialize the classes    
    from cleaning import DataCleaning
    
    connector = DatabaseConnector() 
    cleaner = DataCleaning()
#Read YAML files
    #yaml = connector.read_db_creds(raw_file_path)
    #print(yaml)
    
#Read credentials
    #engine = connector.init_db_engine(raw_file_path)
    #print(engine)

#List database tables
    #tables = connector.list_db_tables(raw_file_path)
    #print(tables)
    #Tables output: ['dim_card_details', 'legacy_store_details', 'legacy_users', 'orders_table']
    
#'USER DATA' -    #Upload Dataframe to table "dim_users" in the database                    
    df_user = cleaner.clean_user_data(raw_file_path)          
    #connector.upload_to_db(df_user, "dim_users", cleaned_file_path) 
    #print("Sucessfuly uploaded 'dim_users' to the database")   
    
 #'CARD DETAILS' - #Upload Dataframe to table table "dim_card_details" in the database
    df_card = cleaner.clean_card_data(raw_file_path)
    #connector.upload_to_db(df_card, "dim_card_details", cleaned_file_path) 
    #print("Sucessfuly uploaded 'dim_card_details' to the database")    
 
 #'SRORE DATA' - #Upload Dataframe to table table "dim_store_details" in the database
    df_store = cleaner.called_clean_store_data(raw_file_path)
    connector.upload_to_db(df_card, "dim_store_details", cleaned_file_path) 
    print("Sucessfuly uploaded 'dim_users' to the database")    


 # 'STORE DATA' #Upload Dataframe to table table "dim_card_details" in the database 
    #raw_creds = connector.read_db_creds(raw_file_path)
    #cleaned_creds = connector.read_db_creds(cleaned_file_path)
    #tables = connector.list_db_tables(raw_file_path)
    #print(tables)
    #Debug output
    #print("Raw creds:", raw_creds)
    #print("Cleaned creds:", cleaned_creds)


#TO DO : 12/07 (last sess), clean Product_details