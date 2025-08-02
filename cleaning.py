import pandas as pd
import re
import openpyxl
from datetime import datetime

raw_file_path = r"E:\projects\multinational\db_creds_raw.yaml"
cleaned_file_path = r"E:\projects\multinational\db_creds_cleaned.yaml"    
    
class DataCleaning:
   
    def clean_user_data(self, raw_file_path):
                
                #Initialize the classes
                from extraction import DataExtractor
                extractor = DataExtractor()
                
                #Load the data from the rds table
                df = extractor.read_rds_table(raw_file_path, table_name= "legacy_users")
                print(type(df.head))
                print(f"{len(df)}")
                
                #convert the joindate column to datetime (format "%Y/%m/%d")
                df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
                
                #Clean date_of_birth column
                df['date_of_birth'] = df['date_of_birth'].str.replace('/','-')

                df.loc[360,'date_of_birth'] = '1968-10-16'
                df.loc[1630,'date_of_birth'] = '1951-01-27'
                df.loc[1997,'date_of_birth'] = '1958-11-11'           
                df.loc[3066,'date_of_birth'] = '1946-10-18'

                df.loc[4205,'date_of_birth'] = '1979-02-01'
                df.loc[5350,'date_of_birth'] = '1943-06-28'
                df.loc[5423,'date_of_birth'] = '1963-11-06'
                df.loc[6108,'date_of_birth'] = '2005-02-05'
                
                df.loc[6221,'date_of_birth'] = '1966-07-08'
                df.loc[7259,'date_of_birth'] = '1948-10-24'
                df.loc[8117,'date_of_birth'] = '1946-12-09'
                df.loc[9935,'date_of_birth'] = '2005-01-27'
                
                df.loc[10246,'date_of_birth'] = '1961-07-14'
                df.loc[11204,'date_of_birth'] = '1939-07-16'
                df.loc[13048,'date_of_birth'] = '1951-01-14'              
                df.loc[14546,'date_of_birth'] = '1996-05-25'

                df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
            
                #Replace "NULL" with actual NaN values     
                null_values = ["NULL", "null", "", " "]
                df.replace(null_values, pd.NA, inplace = True)
                #Drop Null values 
                df = df.dropna(subset=['join_date'])
                #df = df.dropna()
                 
                return df
    
    def clean_card_data(self, raw_file_path):
                  #Initialize the classes
                from extraction import DataExtractor
                extractor = DataExtractor()
                
                df_card = extractor.read_rds_table(raw_file_path, table_name="dim_card_details")
                #print(df_card.columns)
                #Replace "NULL" with actual NaN values     
                null_values = ["NULL", "null", "", " "]
                df_card.replace(null_values, pd.NA, inplace = True)
                #Drop duplicate card numbers
                df_card =df_card.drop_duplicates(subset=['card_number'])
                #Remove non numerical card numbers
                df_card['card_number'] = pd.to_numeric(df_card['card_number'], errors = 'coerce')
                df_card = df_card.dropna(subset = ['card_number'])
                #Convert date_payment_confirmed column to datetime
                df_card['date_payment_confirmed'] = pd.to_datetime((df_card['date_payment_confirmed']), errors='coerce')
                #Drop Null values
                df_card = df_card.dropna()
                return df_card

    def called_clean_store_data(self, raw_file_path):
                from extraction import DataExtractor
                extractor = DataExtractor()

                df_store = extractor.read_rds_table(raw_file_path, table_name="legacy_store_details")
                #print("Before any cleaning:", df_store.shape)
                
                #Replace "NULL" with actual NaN values     
                null_values = ["NULL", "null"]
                df_store.replace(null_values, pd.NA, inplace = True)              
                
                # Replace slashes with dashes
                df_store['opening_date'] = df_store['opening_date'].str.replace('/','-')
                
                # Clean rows in "opening_date" column
                df_store.loc[10,'opening_date'] = '2012-10-08'  
                df_store.loc[11,'opening_date'] = '2015-07-14'    
                df_store.loc[122,'opening_date'] = '2020-02-01'
                df_store.loc[143,'opening_date'] = '2003-05-27'
                df_store.loc[190,'opening_date'] = '2016-11-25'
                               
                df_store.loc[242,'opening_date'] = '2006-10-04'
                df_store.loc[292,'opening_date'] = '2001-05-04'
                df_store.loc[340,'opening_date'] = '1994-11-24'
                df_store.loc[369,'opening_date'] = '2009-02-28'
                df_store.loc[394,'opening_date'] = '2015-03-02'
                               
                # Convert 'opening_date' column to datetime format (converting invalid dates to NaT)      
                df_store['opening_date'] = pd.to_datetime(df_store['opening_date'], errors='coerce')
                
                # Format the dates to 'YYY-MM-DD'
                df_store['opening_date'] = df_store['opening_date'].dt.strftime('%Y-%m-%d')  
                #print("After converting 'opening date to datetime:", df_store.shape)

                #Remove symbols, letters & space from 'staff_number' column
                df_store['staff_numbers'] = df_store['staff_numbers'].astype(str).str.replace(r"[^\d]", "", regex=True)
                df_store['staff_numbers'].replace("", pd.NA, inplace=True)
                #print("After removing symbols/spaces:", df_store.shape)
                                              
                #Drop Null values        
                df_store = df_store.dropna(subset=["staff_numbers", "opening_date"])
                #print("After removing nulls:", df_store.shape)
                
                #print(df_store['staff_numbers'].str.isnumeric().all())                    
                return df_store

    def convert_product_weights(self, s3_path): 
        from extraction import DataExtractor
        extractor = DataExtractor()
        df = extractor.extract_from_s3(s3_path)

        def clean_weight(value):
            if not isinstance(value, str):
                return None

            value = value.lower().replace(" ", "")

            if "x" in value:
                parts = value.split("x")
                if len(parts) != 2:
                    return None
                try:
                    qty = float(parts[0])
                    unit_value = parts[1]

                    if "kg" in unit_value:
                        number = float(unit_value.replace("kg", ""))
                        return qty * number
                    if "ml" in unit_value:
                        number = float(unit_value.replace("ml", ""))
                        return qty * number / 1000
                    if "g" in unit_value:
                        number = float(unit_value.replace("g", ""))
                        return qty * number / 1000
                except:
                    return None

            # Simple cases without 'x'
            try:
                if "kg" in value:
                    return float(value.replace("kg", ""))
                if "g" in value:
                    return float(value.replace("g", "")) / 1000
                if "ml" in value:
                    return float(value.replace("ml", "")) / 1000
            except:
                return None

            return None

        # Apply the cleaning function to the 'weight' column
        df['weight'] = df['weight'].apply(clean_weight)
        df['weight'] = df['weight'].round(2)
        df = df.dropna(subset=["weight"])
        return df


    def clean_orders_data(self,raw_file_path):
        from extraction import DataExtractor
        extractor = DataExtractor()
        
        df = extractor.read_rds_table(raw_file_path, table_name = "orders_table")
        #Remove unwanted columns
        df.drop(columns=["first_name","last_name","1"], inplace = True)
        #print(df.columns)
        return df
    
      
if __name__ == "__main__":
    from extraction import DataExtractor
    extractor = DataExtractor()
    cleaner = DataCleaning()

#'USER DATA'   
    user_df = cleaner.clean_user_data(raw_file_path)
    # Show only date_of_birth values that don't match the pattern
    #print(user_df.loc[~user_df['date_of_birth'].str.match(r'^\d{4}-\d{2}-\d{2}$', na=False), 'date_of_birth'])
    print('User:', user_df.columns)
#'CARD DATA
    df_card = extractor.read_rds_table(raw_file_path, table_name="dim_card_details")
    #print(df_card.columns)
    #Column names: ['index', 'Unnamed: 0', 'card_number', 'expiry_date', 'card_provider','date_payment_confirmed']
    df_cleaned_card = cleaner.clean_card_data(raw_file_path)
    print('card data:', df_cleaned_card.columns)

#'STORE DATA
    #df_store = extractor.read_rds_table(raw_file_path, table_name="legacy_store_details")
    #print(df_store.columns)
    #Print to excel
    #df_cleaned_store.to_excel("df_store.xlsx", index=False)
    df_cleaned_store = cleaner.called_clean_store_data(raw_file_path)
    #row = df_cleaned_store.iloc[[10, 11, 122, 143, 190,242,292,340,369,394, 401]][['opening_date']]
    print('store data:', df_cleaned_store.columns)

#'PRODUCTS
    s3_path = 's3://data-handling-public/products.csv'
    products = cleaner.convert_product_weights(s3_path)
    print('products:', products.columns)
    
#'ORDERS 
    df_orders = cleaner.clean_orders_data(raw_file_path)
    print('orders:', df_orders.columns)