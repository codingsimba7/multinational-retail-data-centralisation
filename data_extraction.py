import yaml
import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import boto3
import io
from typing import Tuple

class DataExtractor:
    def __init__(self):
        self.database_connector = DatabaseConnector()
        self.api_headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        self.user_df = self._read_rds_table(self.database_connector.user_table_name)
        self.orders_df = self._read_rds_table(self.database_connector.orders_table_name)
        self.number_of_stores = self._list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", self.api_headers)
        self.store_data = self.retrieve_stores_data()
        self.products_data = self._fetch_data_from_s3("s3://data-handling-public/products.csv")
        self.sales_data = self._fetch_data_from_s3("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")

    def _read_rds_table(self, table_name: str) -> pd.DataFrame:
        return pd.read_sql_table(table_name, con=self.database_connector.external_db_engine)

    def retrieve_pdf_data(self, link: str) -> pd.DataFrame:
        dfs = tabula.read_pdf(link, pages="all", multiple_tables=True,stream=False)
        return pd.concat(dfs, ignore_index=True)

    def _list_number_of_stores(self, endpoint: str, headers: dict) -> int:
        response = requests.get(endpoint, headers=headers)
        return response.json()["number_stores"]

    def retrieve_stores_data(self) -> pd.DataFrame:
        information = []
        for i in range(0, self.number_of_stores):
            try:
                response = requests.get(f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}", headers=self.api_headers)
                response.raise_for_status()
                information.append(response.json())
            except requests.HTTPError as http_err:
                print(f"HTTP error occurred for store {i}: {http_err}")
            except Exception as err:
                print(f"Other error occurred for store {i}: {err}")

        return pd.DataFrame(information)

    def _fetch_data_from_s3(self, bucket_link: str) -> pd.DataFrame:
        s3 = boto3.client("s3")
        bucket_name, file_key = self._parse_s3_link(bucket_link)

        buffer = io.BytesIO()
        s3.download_fileobj(bucket_name, file_key, buffer)
        buffer.seek(0)

        if ".json" in bucket_link:
            return pd.read_json(buffer)
        elif ".csv" in bucket_link:
            return pd.read_csv(buffer, index_col=0)

    def _parse_s3_link(self, bucket_link: str) -> Tuple[str, str]:
        """Parse S3 link to extract bucket name and file key."""
        # For s3:// formatted links
        if bucket_link.startswith("s3://"):
            parts = bucket_link.replace("s3://", "").split("/")
            bucket_name = parts[0]
            file_key = "/".join(parts[1:])
        # For https:// formatted links
        else:
            parts = bucket_link.split("/")
            # Extracting bucket name from the domain
            bucket_name = parts[2].split(".")[0]
            file_key = "/".join(parts[3:])
        return bucket_name, file_key
