##Old code

from data_extraction import DataExtractor
#from pandasgui import show
import re
import pandas as pd
import sys
from datetime import datetime
import os
import tabula

extractor = DataExtractor()

#sys.path.append(os.path.dirname('D:\Aicore_projects\Data Manipulation\Multinational Retail Data'))
script_dir = r"D:\Aicore_projects\Data Manipulation\Multinational Retail Data"
sys.path.append(script_dir)


pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

#Read data from the PDF and get the combined dataframe



#Perform data cleaning on the PDF DataFrame
#cleaned_data = cleaner.clean_card_data(combined_df)

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
                #Find rows with letters in the "join_date" column 
                filtered_df_2 = df_cleaned[df_cleaned['join_date'].str.contains('[a-zA-Z]', regex=True)]
                #Convert the join date column to the following format: "%Y-%m-%d"
                df_cleaned['join_date'] = pd.to_datetime(df_cleaned['join_date'], errors='coerce')
                #Replace NaT values to the following format:"%Y-%m-%d"
                df_cleaned['join_date'] =  df_cleaned['join_date'].dt.strftime('%Y-%m-%d')            
                
                return  df_cleaned
    
        def clean_card_data(self, pdf_path):
        
                #Extract data from the PDF file 
                combined_df = extractor.retrieve_pdf_data(pdf_path)
                 
                #Remove duplicates from the DataFrame
                pdf_cleaned = combined_df.drop_duplicates().copy()  
       
        #'Card_number' column
                #Convert the "card_number" column to numeric values
                pdf_cleaned['card_number'] = pd.to_numeric(pdf_cleaned['card_number'], errors='coerce')
                #Check if the "card_number" column contains only numbers (not NaN/missing)
                check_only_numbers = pdf_cleaned["card_number"].notna().all()

        #Clean the 'Date_payment_confirmed' column
                #Convert column values to string
                        #Remove rows with non digit values from the column (non numeric values will be converted no NaN)
                pdf_cleaned['date_payment_confirmed'] = pd.to_datetime(pdf_cleaned['date_payment_confirmed'], errors='coerce')
                pdf_cleaned['date_payment_confirmed'] = pdf_cleaned['date_payment_confirmed'].dt.strftime('%Y-%m-%d')

                # Remove NULL values
                pdf_cleaned =  pdf_cleaned.dropna()
                        #cleaner = DataCleaning()                
               
                #return {'card_details_only_numbers': check_only_numbers, 'cleaned_data': pdf_cleaned}
                return pdf_cleaned

        def called_clean_store_data(self,base_url,headers,store_numbers):
                
                #Retrieve and Copy the stored data
                store_data = extractor.retrieve_stores_data(base_url,headers,store_numbers).copy()
                
                #Remove duplicates from the dataframe
                store_data_copy = store_data.drop_duplicates()
                
                #Set 'index' column as the DataFrame's index (change name from index to label)
                store_data_copy = store_data_copy.rename(columns={'index': 'label'})
                store_data_copy = store_data_copy.set_index('label', drop= False)

        #1. 'Address' column - remove whitespaces(from the beginning and end of the string), Capitalize the first letter
                store_data_copy['address'] = store_data_copy['address'].str.strip().str.title()

        #2. 'Longitude' column - Must be numeric, (-180 to 180)                
                #2.1 Identify and convert non numeric numbers in the 'longitude' column
                store_data_copy['longitude'] = pd.to_numeric(store_data_copy['longitude'],errors='coerce')
                #2.2 Check rows where "longitude" column has NaN values
                nan_rows_longitude = store_data_copy[store_data_copy['longitude'].isna()]
                
                #2.3 Longitude values should be between -180 and 180     
                store_data_copy['longitude'] = store_data_copy['longitude'].where(store_data_copy['longitude'].between(-180,180))
                
                #2.4 Round the values to the same decimal place (5 decimal places for precision)
                store_data_copy['longitude'] = store_data_copy['longitude'].round(5)
       
        #3. 'Lat' column# Drop 'lat' column
                store_data_copy.drop(['lat'], axis='columns', inplace=True)

        #4. 'Locality' , (Contains only letters, Capitalize first letter)
                #Replace non alphabetical values to nan (excluding spaces)
                store_data_copy['locality'] = store_data_copy['locality'].where(store_data_copy['locality'].str.match(r'^[a-zA-Z\s\-]*$'))
                #The first letter of each word is capitalized
                store_data_copy['locality'] = store_data_copy['locality'].str.title()
                #Check for and clean 'nan' values in locality column
                nan_rows_locality = store_data_copy[store_data_copy['locality'].isna()]
                #Drop 63,172,231,333,381,414,447
               
        #5. 'Store_code' ?????

        #6. 'Staff_number'     
                        
                #6.1 Clean rows in "staff_numbers" column
                store_data_copy.loc[31,'staff_numbers'] = 78
                store_data_copy.loc[179,'staff_numbers'] = 30
                store_data_copy.loc[248,'staff_numbers'] = 80
                store_data_copy.loc[341,'staff_numbers'] = 97
                store_data_copy.loc[375,'staff_numbers'] = 39
                
                #6.2 "staff_numbers" column (identify and convert non numeric values)
                store_data_copy['staff_numbers'] = pd.to_numeric(store_data_copy['staff_numbers'],errors='coerce')
                #6.3 Check rows where "staff_numbers" column has NaN values
                nan_rows_staff_numbers = store_data_copy[store_data_copy['staff_numbers'].isna()] 
                
                #Drop 63,172,217,231,333,381,405,414,437,447
        #7. 'Opening_date'
                           
                #7.3 Clean rows in "opening_date" column
                store_data_copy.loc[10,'opening_date'] = '2012-10-09'  
                store_data_copy.loc[11,'opening_date'] = '2015-07-14'                
                store_data_copy.loc[122,'opening_date'] = '2020-02-01'
                store_data_copy.loc[143,'opening_date'] = '2003-05-27'
                store_data_copy.loc[190,'opening_date'] = '2016-11-25'
                               
                store_data_copy.loc[242,'opening_date'] = '2006-10-04'
                store_data_copy.loc[292,'opening_date'] = '2001-05-04'
                store_data_copy.loc[340,'opening_date'] = '1994-11-24'
                store_data_copy.loc[369,'opening_date'] = '2009-02-28'
                store_data_copy.loc[394,'opening_date'] = '2015-03-02'

                #7.3 Replace slashes with dashes
                store_data_copy['opening_date'] = store_data_copy['opening_date'].str.replace('/','-')
                #7.2 Convert 'opening_date' column to datetime format (converting invalid dates to NaT)      
                store_data_copy['opening_date'] = pd.to_datetime(store_data_copy['opening_date'], errors='coerce')
                #7.3 Format the dates to 'YYY-MM-DD'
                store_data_copy['opening_date'] = store_data_copy['opening_date'].dt.strftime('%Y-%m-%d')    
          
                #7.2 Find the (NaT) in the 'opening_date' column
                #nan_rows_opening_date = store_data_copy.loc[store_data_copy['opening_date'].isna(),['opening_date']]
                
                #To drop: 63,172,217,231,333,381,405,414,437,447
                #Filter the data frame to show only the rows where the opening_date is NaN
                nan_rows_opening_date = store_data_copy[store_data_copy['opening_date'].isna()]

        #8. 'Store_type' - 
               
                valid_store_types = ['Super Store','Outlet','Mall Kiosk','Local']
                store_data_copy['store_type'] = store_data_copy['store_type'].where(store_data_copy['store_type'].isin(valid_store_types))
                nan_rows_store_type = store_data_copy.loc[store_data_copy['store_type'].isna(),['store_type']]
                
                #Drop 63,172,217,231,333,381,405,414,437,447

        #9. 'Latitude' - Should be numeric, values between -90 & 90
               
                #9.1 Identify and convert non numeric numbers in the 'latitude' column
                store_data_copy['latitude'] = pd.to_numeric(store_data_copy['latitude'],errors='coerce')
                #9.2 Latitude values should be between -90 and 90 (values outside the range are converted to nan)    
                store_data_copy['latitude'] = store_data_copy['latitude'].where(store_data_copy['latitude'].between(-90,90))
                #9.3 Round the values to the same decimal place (5 decimal places for precision)
                store_data_copy['latitude'] = store_data_copy['latitude'].round(5)
                #9.4 Check rows where "longitude" column has NaN values
                nan_rows_latitude = store_data_copy.loc[store_data_copy['latitude'].isna(),['latitude']]
                
                #9.5 Deal with the nan values
                
                #Drop 63 - Some of the latitude values are outside of the range, drop or fill?????

        #10. 'Country_code'
                valid_country_codes = ['DE','GB','US']
                store_data_copy['country_code'] = store_data_copy['country_code'].where(store_data_copy['country_code'].isin(valid_country_codes))
                nan_country_code = store_data_copy.loc[store_data_copy['country_code'].isna(),['country_code']]
        
        #11  'Continent'  
                valid_continents = ['Europe','America','Africa','Asia', 'Oceania']
                #Fixing typos
                store_data_copy['continent'] = store_data_copy['continent'].str.replace('eeEurope','Europe')
                store_data_copy['continent'] = store_data_copy['continent'].str.replace('eeAmerica','America')
               
                #Any values outside of valid_continents are converted to nan    
                store_data_copy['continent'] = store_data_copy['continent'].where(store_data_copy['continent'].isin(valid_continents))
                
                #nan_continent = store_data_copy.loc[store_data_copy['continent'].isna(),['continent']]
                
             
                #1. Identify missing values
                #store_data_null = store_data.isnull()
                #2. Remove  rows with NULL values
                #store_null_list = store_data.dropna()
                
                #3. Remove duplicates
                #store_cleaned =  store_cleaned.drop_duplicates()
                
                #4. Standardize capitalization
                #5. Convert data type
                #6. Handle missing values
                #1.4 Remove Null values 
                #store_data_copy = store_data_copy.dropna()
                #return 
                #print(store_data_copy['longitude'].head(20))
                print(nan_rows_opening_date)
                #print(store_data_copy['store_type'].unique())
                
## ****Left to do Milestone 2, Task4 => 1.Check if the cc only contain numbers 2.Upload the table to the database **

       
if __name__ == "__main__":
        
        extractor = DataExtractor()
        cleaner = DataCleaning()

        pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"       
        #combined_df = extractor.retrieve_pdf_data(pdf_path)
        #cleaned_data = cleaner.clean_card_data(pdf_path)
        file_path = r"D:\Aicore_projects\Data Manipulation\Multinational Retail Data\db_creds.yaml"
        
        #credentials = read_db_creds(file_path)
        #engine = init_db_engine()
        #tables = extractor.list_db_tables(file_path)
        user_table_name = "legacy_users"
        #df = extractor.read_rds_table(file_path,user_table_name)
        #df_cleaned = cleaner.clean_user_data(df)
        #pdf_cleaned = combined_df.drop_duplicates().copy() 
        #check_only_numbers = pdf_cleaned["card_number"].notna().all()
        
        #Call the "Called_clean_store_data" function#      
        
        base_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
        headers = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        store_numbers = range(1,451)
        clean_data = cleaner.called_clean_store_data(base_url,headers,store_numbers)
        #lenght = len(clean_data)
        
        #print(clean_data.loc[[31,179,248,341,375], 'staff_numbers'])
        #clean_data.to_excel("store_data_new.xlsx", index = False) 
        
        #Print the statements to inspect the dataframe
        #print(clean_data.head(20))
        print(clean_data)
       
        #gui = show(clean_data)
        #print(clean_data)

        #Values that should not be removed: 31, 179, 248,341,375 (These have been changed in the staff_numbers column)


