import re
import pandas as pd
import sys
from datetime import datetime
import os
import tabula
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
#sys.path.append(os.path.dirname('D:\Aicore_projects\Data Manipulation\Multinational Retail Data'))
script_dir = r"D:\Aicore_projects\Data Manipulation\Multinational Retail Data"
sys.path.append(script_dir)
pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

#Create an instance of the DataExtractor class
extractor = DataExtractor()
#Read data from the PDF and get the combined dataframe
combined_df = extractor.retrieve_pdf_data(pdf_path)

from data_cleaning import DataCleaning
cleaner = DataCleaning()

#Perform data cleaning on the PDF DataFrame
cleaned_data = cleaner.clean_card_data(combined_df)

#(Milestone2, Task2, Step3)
#This class will be used to clean data from each of the data sources

class DataCleaning:
        def __init__(self):
                pass

#In this step we will clean the user data (df = dataframe)
        def clean_user_data(self, df):
             
        #Get the Null values
                df_null = df.isnull()
        #Drop Null values
                df_cleaned = df.dropna()
        #Drop duplicate rows
                df_cleaned =  df_cleaned.drop_duplicates()
        #Clean date of birth column
                #Find the rows with dates in the following format: "%Y/%m/%d"
                filtered_df2 = df_cleaned[df_cleaned['date_of_birth'].str.contains("/")]
                #Find rows with letters in the "date_of_birth" column 
                filtered_df2 = df_cleaned[df_cleaned['date_of_birth'].str.contains('[a-zA-Z]', regex=True)]
                #Convert the date of birth column to the following format: "%Y-%m-%d"
                df_cleaned['date_of_birth'] = pd.to_datetime(df_cleaned['date_of_birth'], errors='coerce')
                #Convert the datetime values to the following format: %Y-%m-%d
                df_cleaned['date_of_birth'] =  df_cleaned['date_of_birth'].dt.strftime('%Y-%m-%d')
                
        
        #Clean the joindate column
                ##Find the rows with dates in the following format: "%Y/%m/%d"
                filtered_df2 = df_cleaned[df_cleaned['join_date'].str.contains("/")]
                #Find rows with letters in the "date_of_birth" column 
                filtered_df_2 = df_cleaned[df_cleaned['join_date'].str.contains('[a-zA-Z]', regex=True)]
                #Convert the join date column to the following format: "%Y-%m-%d"
                df_cleaned['join_date'] = pd.to_datetime(df_cleaned['join_date'], errors='coerce')
                #Replace NaT values to the following format:"%Y-%m-%d"
                df_cleaned['join_date'] =  df_cleaned['join_date'].dt.strftime('%Y-%m-%d')            
                
                return  df_cleaned
    
        def clean_card_data(self,  combined_df):
                #combined_df = extractor.retrieve_pdf_data(pdf_path)
                
                # Remove NULL values
                pdf_cleaned =  combined_df.dropna()
                #Remove duplicates
                pdf_cleaned = pdf_cleaned.drop_duplicates()
        #Clean the 'date_payment_confirmed' column
                #Find rows with dates in the following format: "%Y/%m/%d" 
                filtered_pdf = pdf_cleaned[pdf_cleaned['date_payment_confirmed'].str.contains("/", na=False)]
                #Standardize the "date_payment_confirme" to the following format:"%Y-%m-%d".
                pdf_cleaned['date_payment_confirmed'] = pd.to_datetime(pdf_cleaned['date_payment_confirmed'], errors='coerce')
                pdf_cleaned['date_payment_confirmed'] = pdf_cleaned['date_payment_confirmed'].dt.strftime('%Y-%m-%d')
        #Check if card details only contain numbers
                check_only_numbers = pdf_cleaned["card_number"].str.isdigit().all()


                return  check_only_numbers               



if __name__ == "__main__":
        file_path = r"D:\Aicore_projects\Data Manipulation\Multinational Retail Data\db_creds.yaml"
        pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        extractor = DataExtractor()
        cleaner = DataCleaning()
        combined_df = extractor.retrieve_pdf_data(pdf_path)
        cleaned_data = cleaner.clean_card_data(combined_df)
        #credentials = read_db_creds(file_path)
        #engine = init_db_engine()
        tables = extractor.list_db_tables(file_path)
        user_table_name = "legacy_users"
        df = extractor.read_rds_table(file_path,user_table_name)
        df_cleaned = cleaner.clean_user_data(df)
        
       
        print("Card details only contain numbers:", cleaned_data)   
   
       
        #extractor = DataExtractor
