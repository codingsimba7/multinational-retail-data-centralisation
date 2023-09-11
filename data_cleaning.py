import pandas as pd
import numpy as np
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import re

# logs to check if everything is working not needed all the time
# import logging
# logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

## this function will preprocess the date of birth column. Initially, I attempted to automatically, convert the column using pd.to_datetime, 
# 
#  which will convert any dates that are not in the correct format to np.nan. Upon investigating the rows that have the null values, 
# it seems that the entire row has the same value which obviously is a mistake, thereofre, I decided to drop the rows with null values,
def preprocess_date(date_column):
    dates=[]
    for i in date_column:
        try:
            dates.append(pd.to_datetime(i))
            # print(pd.to_datetime(i))
        except:
            # print(i)
            dates.append(pd.to_datetime(i,errors="coerce"))
    return dates

def preprocess_expiry_date(date_column):
    dates=[]
    for i in date_column:
        date=pd.to_datetime(i,format="%m/%y",errors="coerce")
        final_date_format=date+pd.offsets.MonthEnd(0)
        dates.append(final_date_format)
    return dates

def preprocess_card_provider(card_provider_column):
    card_providers = []
    
    for i in card_provider_column:
        if any(char.isdigit() for char in i):
            card_name = [name for name in i.split(" ") if not name.isdigit() and name != "digit"]
            card_providers.append(" ".join(card_name))
        else:
            card_providers.append(i)
            
    return card_providers


class DataCleaning():
    def __init__(self):
        self.data_extractor=DataExtractor()
        self.data_connector=DatabaseConnector()
        self.user_df=self.data_extractor.user_df
        self.card_data=self.data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        self.product_data=self.data_extractor.products_data
        self.store_data=self.data_extractor.retrieve_stores_data()
        self.order_data=self.data_extractor.orders_df
        self.sales_data=self.data_extractor.sales_data
        self.user_df_clean=self.clean_user_data()
        self.card_data_clean=self.clean_card_data()
        self.product_data_clean=self.clean_products_data()
        self.store_data=self.clean_store_data()
        self.order_data_clean=self.clean_orders_data()
        self.sales_data_clean=self.clean_sales_data()
        self.upload_data()

    def clean_user_data(self):
        self.user_df.replace("NULL",np.nan,inplace=True)
        self.user_df.dropna(inplace=True)
        self.user_df["date_of_birth"]=preprocess_date(self.user_df["date_of_birth"])
        self.user_df.dropna(inplace=True)
        self.user_df["join_date"]=preprocess_date(self.user_df["join_date"])
        return self.user_df


    def clean_card_data(self):
        self.card_data.replace("NULL",np.nan,inplace=True)
        self.card_data.dropna(subset=["card_number"],inplace=True)
        self.card_data["date_payment_confirmed"]=preprocess_date(self.card_data["date_payment_confirmed"])
        self.card_data.dropna(subset=["date_payment_confirmed"],inplace=True)
        self.card_data["expiry_date"]=preprocess_expiry_date(self.card_data["expiry_date"])
        self.card_data["card_provider"]=preprocess_card_provider(self.card_data["card_provider"])
        return self.card_data
    
    def clean_store_data(self):
        store_data=self.store_data
        store_data.drop(columns=["lat"],inplace=True)
        filtered=(store_data[store_data.drop(columns="index").isnull().all(axis=1)].index)
        store_data=store_data.drop(filtered)
        store_data = store_data[~store_data['store_type'].str.contains(r'\d')]
        store_data["staff_numbers"]=store_data["staff_numbers"].str.replace(r'\D+', '',regex=True)
        store_data["continent"] = store_data["continent"].str.replace(r'ee', '', regex=True)
        store_data["opening_date"]=preprocess_date(store_data["opening_date"])

        return store_data
    
    def clean_orders_data(self):
        self.order_data=self.order_data.drop(columns=["1","first_name","last_name","level_0"])
        return self.order_data
    
    def clean_sales_data(self):
        self.sales_data.replace("NULL",np.nan,inplace=True)
        self.sales_data.dropna(inplace=True)
        rows_to_drop=(self.sales_data[self.sales_data["time_period"].str.contains(r'\d')].index)
        self.sales_data.drop(rows_to_drop,inplace=True)
        return self.sales_data
    
    def convert_product_weights(self):
        weights = self.product_data["weight"].tolist()
        modified = []
        for w in weights:
            if "x" in str(w):
                quantity, weight = w.split("x")
                weight = weight.strip("g")
                new_weight=float(weight)*float(quantity)/1000
                modified.append(new_weight)
            elif "oz" in str(w):
                new_weight=float(w.strip("oz"))/0.0283495
                modified.append(new_weight)
            elif "kg" in str(w):
                new_weight=float(w.strip("kg"))
                modified.append(new_weight)
            elif "g" in str(w):
                new_weight =float(re.sub(r'\D', '', w))/1000
                modified.append(new_weight)
            elif "ml" in str(w):
                new_weight =float(re.sub(r'\D', '', w))/1000
                modified.append(new_weight)
            else:
                w=np.nan
                modified.append(w)
        self.product_data["weight"]=modified
        return self.product_data
    def clean_products_data(self):
        self.product_data=self.convert_product_weights()
        self.product_data.dropna(inplace=True)
        self.product_data["date_added"]=preprocess_date(self.product_data["date_added"])
        return self.product_data
    

    
    def upload_data(self):
        self.data_connector.upload_to_db(self.user_df_clean, "dim_users")
        self.data_connector.upload_to_db(self.card_data, "dim_card_details")
        self.data_connector.upload_to_db(self.store_data, "dim_store_details")
        self.data_connector.upload_to_db(self.product_data_clean, "dim_products")
        self.data_connector.upload_to_db(self.order_data_clean, "orders_table")
        self.data_connector.upload_to_db(self.sales_data_clean, "dim_date_details")




    

bebsi=DataCleaning()




    
"""
examingin the card_data there are few observations to be made and will be addressed in the cleaning
1. there are 11 null values, and are in the card number and and date_payment confirmed, I will see if they are in the same
rows and drop them if they are, nonetheless, I will drop card_number nulls as withouth the card number the data is useless. But first the null
should be changed to np.nan. Once the nan values were dropped the nulls in the date_payment_confirmed were also dropped indicating that they were in the same rows
expiry_date and date_payment confirmed should be changed to datetime objects using the preprocess_date function. Expiry date change has turned them all into NaT, so I will have to
revaluate how to change the expiry date column, with another function
When changing the date_payment_confirmed we see that there are new null values and upon exmainig these rows they entire row is filled with wrong information
so I will drop these rows
I want to check if there are any numbers in the card_provider information and possibly change it, to simply include the name of the card provider
"""
