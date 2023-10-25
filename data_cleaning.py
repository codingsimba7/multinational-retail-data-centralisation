import pandas as pd
import numpy as np
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import re
import logging
from uuid import UUID
# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataCleaning:
    def __init__(self):
        logging.info("Initializing DataCleaning class.")
        
        self.data_extractor = DataExtractor()
        self.data_connector = DatabaseConnector()
        
        # Retrieve and clean data
        logging.info("Cleaning user data.")
        self.user_df = self.clean_user_data(self.data_extractor.user_df)

        logging.info("Cleaning card data.")
        self.card_data = self.clean_card_data(self.data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"))

        logging.info("Cleaning product data.")
        self.product_data = self.clean_products_data(self.data_extractor.products_data)

        logging.info("Cleaning store data.")
        self.store_data = self.clean_store_data(self.data_extractor.retrieve_stores_data())

        logging.info("Cleaning order data.")
        self.order_data = self.clean_orders_data(self.data_extractor.orders_df)

        logging.info("Cleaning sales data.")
        self.sales_data = self.clean_sales_data(self.data_extractor.sales_data)
        
        # Upload cleaned data
        logging.info("Uploading cleaned data to the database.")
        self.upload_data()

    @staticmethod
    def _preprocess_date(date_column: pd.Series) -> pd.Series:
        """Preprocess date column."""
        dates = []
        for i in date_column:
            try:
                dates.append(pd.to_datetime(i))
            except:
                dates.append(pd.to_datetime(i, errors="coerce"))
        return pd.Series(dates)

    @staticmethod
    def preprocess_expiry_date(date_column: pd.Series) -> pd.Series:
        return [(pd.to_datetime(i, format="%m/%y", errors="coerce") + pd.offsets.MonthEnd(0)) for i in date_column]

    @staticmethod
    def preprocess_card_provider(card_provider_column: pd.Series) -> pd.Series:
        card_providers = [
            " ".join(name for name in i.split(" ") if not name.isdigit() and name != "digit")
            if any(char.isdigit() for char in i) else i
            for i in card_provider_column
        ]
        return pd.Series(card_providers)

    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df.replace("NULL", np.nan, inplace=True)
        df.dropna(inplace=True)
        df["date_of_birth"] = self._preprocess_date(df["date_of_birth"])
        df.dropna(inplace=True)
        df = df[~df['first_name'].str.contains(r'\d')]
        df["join_date"] = self._preprocess_date(df["join_date"])
        return df

    def clean_card_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df.replace("NULL", np.nan, inplace=True)
        df.dropna(subset=["card_number"], inplace=True)
        df["date_payment_confirmed"] = self._preprocess_date(df["date_payment_confirmed"])
        # df["expiry_date"] = self.preprocess_expiry_date(df["expiry_date"])
        df["card_provider"] = self.preprocess_card_provider(df["card_provider"])
        return df

    def clean_store_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df.replace("NULL", np.nan, inplace=True)
        df.replace("N/A", np.nan, inplace=True)
        df.dropna(subset=df.columns.difference(["index"]), how='all', inplace=True)
        df = df[~df['store_type'].str.contains(r'\d')]
        df["staff_numbers"] = df["staff_numbers"].str.replace(r'\D+', '', regex=True)
        df["continent"] = df["continent"].str.replace(r'ee', '', regex=True)
        df["opening_date"] = self._preprocess_date(df["opening_date"])
        return df

    def clean_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop(columns=["1", "first_name", "last_name", "level_0"])
        return df

    def clean_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df.replace("NULL", np.nan, inplace=True)
        df.dropna(inplace=True)
        rows_to_drop = df[df["time_period"].str.contains(r'\d')].index
        df.drop(rows_to_drop, inplace=True)
        return df

    def convert_product_weights(self, df: pd.DataFrame) -> pd.DataFrame:
        weights = df["weight"].tolist()
        modified = []
        for w in weights:
            if "x" in str(w):
                quantity, weight = w.split("x")
                weight = weight.strip("g")
                new_weight = float(weight) * float(quantity) / 1000
                modified.append(new_weight)
            elif "oz" in str(w):
                new_weight = float(w.strip("oz")) / 0.0283495
                modified.append(new_weight)
            elif "kg" in str(w):
                new_weight = float(w.strip("kg"))
                modified.append(new_weight)
            elif "g" in str(w):
                new_weight = float(re.sub(r'\D', '', w)) / 1000
                modified.append(new_weight)
            elif "ml" in str(w):
                new_weight = float(re.sub(r'\D', '', w)) / 1000
                modified.append(new_weight)
            else:
                modified.append(np.nan)
        df["weight"] = modified
        return df

    def clean_products_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.convert_product_weights(df)
        df.dropna(inplace=True)
        df["date_added"] = self._preprocess_date(df["date_added"])
        return df

    def upload_data(self):
        logging.info("Uploading user data to database.")
        self.data_connector.upload_to_db(self.user_df, "dim_users")

        logging.info("Uploading card data to database.")
        self.data_connector.upload_to_db(self.card_data, "dim_card_details")

        logging.info("Uploading product data to database.")
        self.data_connector.upload_to_db(self.product_data, "dim_products")

        logging.info("Uploading store data to database.")
        self.data_connector.upload_to_db(self.store_data, "dim_store_details")

        logging.info("Uploading order data to database.")
        self.data_connector.upload_to_db(self.order_data, "orders_table")

        logging.info("Uploading sales data to database.")
        self.data_connector.upload_to_db(self.sales_data, "dim_date_times")

        logging.info("Data upload completed.")

bebsi = DataCleaning()
