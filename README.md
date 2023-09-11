# multinational-retail-data-centralisation
## Table of Contents
-[Description](#description)
-[Installation](#installation)
-[Usage](#usage)
-[File Structure](#file-structure)
-[License](#license)

## Description
### Goal of the Project
The goal of this project is to play the role of a analytics engineer at a multinational company that sells various goods across the globe. Before starting this project, the sales data was spread across many different data sources. making it difficult to access and analyse from one centralised location. The goal of this project is to first, produce a system that stores the current company data in a databse so that its accessed from one centralised location and act as a single source of truth for sales data. Secondly, I will query the database, analyse the data and get up to date metrics for the business.

#### Different Data Sources
1. RDS Database
2. 2 Different S3 Buckets
3. 2 Different APIs
4. PDF data

To achieve the first goal, I created three separate Python scripts:
1. [database_utils.py](database_utils.py)
2. [data_extraction.py](data_extraction.py)
3. [data_cleaning.py](data_cleaning.py)

#### database_utils.py
This script contains a class called DatabaseConnector, which is used to connect to the multiple datasources of the business, as well as connecting to the database that will be used as the source of truth. Furthermore, it pushes the cleaned data to the database.

Required libraries:
- sqlalchemy
- yaml 

The script has the following methods: 
1. Initialises the class, it takes as an argument the database credentials for the RDS database, as well as the credentials for the centralised database. It initialises the following attributes:
    - 'self.external_db_creds'- the credentials for the external database which leverages read_db_creds method
    - 'self.external_db_engine'- the engine for the external database which leverages init_db_engine method
    - 'self.internal_db_creds'- the credentials for the internal database which leverages read_db_creds method
    - 'self.internal_db_engine'- the engine for the internal database which leverages init_local_db_engine method
    - 'self.rds_tables'- which returns a list of the tables in the RDS database leveraging the get_rds_tables method
    - 'self.user_table_name'- which returns the name of the table in the RDS database that contains the user_table name, leveraging the list_rds_tables_method
    - 'self.orders_table_name'- which returns the name of the table in the RDS database that contains the orders_table name, leveraging the list_rds_tables_method
2. read_db_creds method, which takes as an argument the name of the yaml file that contains the credentials for the database. It returns a dictionary with the credentials.
3. init_db_engine method, which takes as an argument the credentials for the database. It returns an engine for the database. It leverages the create_engine method from the sqlalchemy library.
4. init_local_db_engine method, which takes as an argument the credentials for the database. It returns an engine for the database. It leverages the create_engine method from the sqlalchemy library.
5. list_rds_tables method, which returns a list of the tables in the RDS database. The name of engine is passed as an argument. It returns a list of the tables in the database, as well as the name for the user_table and orders_table. It leverages the inspect method from the sqlalchemy library as well as the get_table_names method from the inspect object.
6. local_db_engine method, which returns the enginer for the local database. The credentials for the local database are passed as an argument. It leverages the create_engine method from the sqlalchemy library.
7. upload_to_db method, which takes as an argument the dataframe that will be pushed to the database, as well as the name of the table in the database. It pushes the dataframe to the database. It leverages the to_sql method from the pandas library. Furthermore, for testing purposes, it prints out a statement that the data has been successfully pushed to the database.
8. test_connection method, although this is not required to run the code, it was helpful in debugging, to make sure that I have in fact been able to connect to the different databases. It takes as an argument the engine for the database. It returns a statement that the connection has been successful. If not successful it returns a statement that the connection has not been successful along with the error message.

#### data_extraction.py
This script contains a class called DataExtractor, which is used to extract the data from the different data sources. It leverages the DatabaseConnector class from the database_utils.py script.

Required libraries:
 - pandas
 - tabula
 - requests
 - boto3
 - io

The script has the following methods:
1. Initialises the class, It initialises the following attributes:
    - 'self.db_connector'- the DatabaseConnector class, which is used to connect to the different databases, this is done so that the class can inherit the attributes and methods from the DatabaseConnector class
    - 'self.user_df'- this leveragaes the read_rds_table method to return the user_table dataframe
    - 'self.orders_df'- this leveragaes the read_rds_table method to return the orders_table dataframe
    - self.number_of_stores- this leverages the list_number_of_stores method to return the number of stores in the business
    - self.store_data- this leverages the retrieve_store_data method to return the store data
    - self.products_data- this leverages the extract_from_s3 method to return the products data
    - self.sales_data- this leverages the extract_from_s3 method to return the sales data
2. read_rds_table method, which takes as an argument the name of the table in the RDS database. It returns a dataframe with the data from the table. It leverages the read_sql_table method from the pandas library, which calls on the external_db_enginge attribute from the DatabaseConnector class.
3. retrieve_pdf_data method, which takes as an argument the url of the pdf file. It returns a dataframe with the data from the pdf file. It leverages the read_pdf method from the tabula library.
4. list_number_of_stores method, which returns the number of stores in the business. This information is extracted through the use of an API, therefore the API endpoint, as well as the API key are fed into the function. It leverages the get method from the requests library. 
5. retrieve_store_data method, which returns the store data. This information is extracted through the use of an API, furthermore, it requires the number of stores in the business. The way the API works is that it returns the data for each store individually, therefore, the function loops through the number of stores and appends the data to a list. It leverages the get method from the requests library. reponse.raise_for_status() is used to check if the request was successful. As well as including a try and except block to catch any errors. More specifically, the except block will catch any HTTP errors that occur and print out the error message, as well as other errors in a different except block. 

6. extract_from_s3 method, which takes as an argument the url of the file in the s3 bucket. It leverages the download_fileobj method from the boto3 library to download the file from the s3 bucket. Furthermore, it leverages the read_csv method from the pandas library to read the csv file into a dataframe. I've also leveraged io.BytesIO to read the file into a buffer. This is because the read_csv method requires a file-like object, therefore, I've used io.BytesIO to convert the file into a file-like object. We have two different url files, with different formating therefore an if statement was used to detence if json was in the url or if csv was in the url, and th URL was parsed accordingly to retreive the bucket name, file key and file name.

#### data_cleaning.py
This script contains a class called DataCleaner, which is used to clean the data from the different data sources. It leverages both the DataExtractor class from the data_extraction.py script as well as the DatabaseConnector class from the database_utils.py script.

Required libraries:
- pandas
- numpy
- re

The script has the following methods:

1. Initializes the class, and sets up the following attributes: 
    - self.data_extractor- this leverages the DataExtractor class from the data_extraction.py script
    - self.data_connector- this leverages the DatabaseConnector class from the database_utils.py script
    - self.user_df- this leverages the self.data_extractor attribute and then the user_df attribute from the DataExtractor class, to save the dataframe
    - self.card_data- this leverages the self.data_extractor attribute and then the retrieve_pdf_data method from the DataExtractor class, to save the dataframe
    - self.product_data- this leverages the self.data_extractor attribute and then the products_data attribute from the DataExtractor class, to save the dataframe
    - self.store_data- this leverages the self.data_extractor attribute and then the store_data attribute from the DataExtractor class, to save the dataframe
    - self.order_data- this leverages the self.data_extractor attribute and then the orders_df attribute from the DataExtractor class, to save the dataframe
    - self.sales_data- this leverages the self.data_extractor attribute and then the sales_data attribute from the DataExtractor class, to save the dataframe
    - self.user_df_cleaned- this leverages the clean_user_data method to save the cleaned dataframe
    - self.card_data_cleaned- this leverages the clean_card_data method to save the cleaned dataframe
    - self.product_data_cleaned- this leverages the clean_product_data method to save the cleaned dataframe
    - self.store_data_cleaned- this leverages the clean_store_data method to save the cleaned dataframe
    - self.order_data_cleaned- this leverages the clean_order_data method to save the cleaned dataframe
    - self.sales_data_cleaned- this leverages the clean_sales_data method to save the cleaned dataframe
    -self.upload_data- this leverages the upload_data method, to upload the cleaned data to the database
2. clean_user_data- this method cleans the user_data dataframe, while inspecting and analyzing the data and any inconsitencies, as well as any missing values. I decided to replace Null values with NaN values. Furthermore, it was noticed that all the NaN values occured in the same rows, therefore I then drop these values. The next thing that was done was to change both the date_of_birth and the join_date columns to datetime objects, to make it easier to make comparisons and query in the future. Initially, I tried to use pd.to_datetime however, this resulted in an error, as some of the dates were not in the correct format. Hence, I decided to preprocess the dates using the preprocess_date function, parsing through each item and converting them to datetime, and then using the errors="coerce" argument. After that, I reinvestigated the null values, and realized that these rows had the same value all across the row and were obviously data entry mistakes that weren't consistent with their respective column, and were dropped. Then the join date went through the same preprocess_date function
3. clean_card_data- this method cleans the user_data dataframe, while inspecting and analyzing the data and any inconsitencies, as well as any missing values. I decided to replace Null values with NaN values. I then droped all NaN values in the card_number column, as without the card_number the data is not beneficial. I then had to change the date_payment_confirmed, expiry_data to datetime objects. for the date_payment_confirmed, the same prerpocess_date function was used, however, for the expiry_date, I had to create a different function, as it only followed the MM/YY format so I had to approach it in a different way and create the preprocess_expiry_date function. I then reinvestigated the null values, and realized that these rows had the same value all across the row and were obviously data entry mistakes that weren't consistent with their respective column, and were dropped. Finally, upon inspecting the card_provider information, I realized that they included the number of digits for the different Visa cards, therefore I decided to remove the number of digits from the card_provider column, and only keep the card_provider name.
4. clean_store_data, the first thing that was done after inspecting the dataframe, was drop the "lat" column as it was all null values, and seems to be a data entry mistake. Furthermore, after inspecting I noticed there were rows that had all NaN values, and therfore these were dropped. Then I noticed that in store_type there were rows that followed a pattern that were clearly another data entry mistake. They all included a digit as opposed to the correctly entered data, and therfore any rows that included a digit were dropped. While investigating the staff numbers, I noticed some of them included characters which again seems to be a data entry mistake, so I simply replaced the characters with nothing. ANother thing I noticed was that some values for continent included the character "ee", this was removed to unify all entries. Finally I used the preprocess_date function to change the opeinging_date column to a datetime object. 
5. clean_orders_data: only dropped columns that were not needed
6. clean_sales_data: replaces Null with NAN, then drops these values as again they are all in the same rows, furthermore the decision was mad to drop rows in time_period that contined a digit, as this seemed to be another data entry mistake.
7. conver_product_weights, this is done to unify the product weights in the product_data datafame, some values followed this patter 3 x 50g, so I first tackled this issue and multiplied the 3 by the 50 to get the total weight and then converted it to KG, while also dropping the "g", values with oz were converted to kg and dropped "oz", values with KG, simply dropped "KG", values with only "g" were converted to kg and then the "g" was dropped, values with "ml" assumed a 1:1 conversion to grams, so they were converted to kg and the "ml" was dropped, while empty weights were given the value null. 
8. clean_products_data, first calls on the convert_product_weights function, and then drops NaN values, and then leverages the preprocess_date function to change the date_added column to a datetime object.
9. upload_data, this leverages the upload_to_db method from the DatabaseConnector class to upload the cleaned data to the database.



## Installation

## Usage

The code has been set up as of now, where you only need to call the DataCleaning() class and it will run all methods, in all three scripts, and get the final result, which is the cleaned data uploaded to the correct database, which will act as a central source of truth.

## File Structure

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.