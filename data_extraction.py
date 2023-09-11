import yaml
import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import boto3
import io
##importing the database connector class from the database_utils.py file so that we can use an instance of the class to connect to the database

class DataExtractor():
    ## initialize the database connector class and the database and the user database so that each instance of the class will have the database and the user database
    def __init__(self):
        self.database_connector=DatabaseConnector()
        self.api_dictionary={"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        # self.databse_names=self.database_connector.rds_tables
        self.user_df=self.read_rds_table(self.database_connector.user_table_name)
        self.orders_df=self.read_rds_table(self.database_connector.orders_table_name)
        self.number_of_stores=self.list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores",self.api_dictionary)
        self.store_data=self.retrieve_stores_data()
        self.products_data=self.extract_from_s3("s3://data-handling-public/products.csv")
        self.sales_data=self.extract_from_s3("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
    ## read the database table
    def read_rds_table(self,table_name):
        return pd.read_sql_table(table_name,con=self.database_connector.external_db_engine)
    
    def retrieve_pdf_data(self,link):
        dfs=tabula.read_pdf(link, pages="all",multiple_tables=True)
        unified_df=pd.concat(dfs,ignore_index=True)
        return unified_df
    def list_number_of_stores(self,number_of_stores_end_point,dictionary):
        response=requests.get(number_of_stores_end_point,headers=dictionary)
        number_of_stores=response.json()["number_stores"]
        return number_of_stores
    def retrieve_stores_data(self):
        information = []
        for i in range(1,self.number_of_stores+1):
            try:
                response = requests.get(f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}", headers=self.api_dictionary)
                response.raise_for_status()
                information.append(response.json())
            except requests.HTTPError as http_err:
                print(f"HTTP error occurred for store {i}: {http_err}")
            except Exception as err:
                print(f"Other error occurred for store {i}: {err}")

        return pd.DataFrame(information)
    def extract_from_s3(self,bucket_name):
        self.s3=boto3.client("s3")
        if "json" in bucket_name:
            bucket_domain = bucket_name.split("/")[2]
            self.bucket_name = bucket_domain.split(".")[0]
            self.file_key = "/".join(bucket_name.split("/")[3:])
            buffer=io.BytesIO()
            self.s3.download_fileobj(self.bucket_name,self.file_key,buffer)
            buffer.seek(0)
            return pd.read_json(buffer)

        
        if "csv" in bucket_name:
            self.bucket_name=bucket_name.split("/")[2]
            self.file_key="/".join(bucket_name.split("/")[3:])
            buffer=io.BytesIO()
            self.s3.download_fileobj(self.bucket_name,self.file_key,buffer)
            buffer.seek(0)
            return pd.read_csv(buffer)


# bebsi=DataExtractor()

# bebsi.sales_data.replace("NULL",np.nan,inplace=True)
# bebsi.sales_data.dropna(inplace=True)
# rows_to_drop=(bebsi.sales_data[bebsi.sales_data["time_period"].str.contains(r'\d')].index)
# bebsi.sales_data.drop(rows_to_drop,inplace=True)

