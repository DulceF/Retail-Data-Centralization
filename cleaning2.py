import pandas as pd
from extraction import DataExtractor
from utilis import DatabaseConnector

raw_file_path = r"E:\projects\multinational\db_creds_raw.yaml"
cleaned_file_path = r"E:\projects\multinational\db_creds_cleaned.yaml"    
    
class DataCleaning:
        
    def __init__(self, extractor,connector):
            self.extractor = extractor
            self.connector = connector

    def clean_user_data(self, raw_file_path):
                #Load the data from the rds table
                user_df = self.extractor.read_rds_table(raw_file_path, table_name= "legacy_users")
                print(type(user_df))
            
                return user_df
            
if __name__ == "__main__":
        #Initialize the classes
    connector = DatabaseConnector()
    extractor = DataExtractor(connector)
    cleaner = DataCleaning(extractor, connector)
    
    user_df = cleaner.clean_user_data(raw_file_path)
    print(user_df)