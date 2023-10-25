import yaml
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine

class DatabaseConnector:
    """
    Provides functionality to connect to a database and manage its operations.
    """

    def __init__(self, external_database_file: str = "db_creds.yaml",
                 internal_database_file: str = "local_db_creds.yaml") -> None:
        """
        Initializes the DatabaseConnector with necessary attributes by calling helper methods.
        
        :param external_database_file: YAML file containing credentials for the external database.
        :param internal_database_file: YAML file containing credentials for the internal database.
        """
        self.external_db_creds = self._read_db_creds(external_database_file)
        self.external_db_engine = self._init_db_engine(self.external_db_creds)
        self.rds_tables, self.user_table_name, self.orders_table_name = self._list_rds_table(self.external_db_engine)
        self.internal_db_creds = self._read_db_creds(internal_database_file)
        self.internal_db_engine = self._init_local_db_engine(self.internal_db_creds)

    def _read_db_creds(self, file: str) -> dict:
        """
        Reads the database credentials from a YAML file.
        
        :param file: Path to the YAML file containing the database credentials.
        :return: Dictionary containing the database credentials.
        """
        with open(file) as f:
            return yaml.safe_load(f)

    def _init_db_engine(self, db_creds: dict) -> 'Engine':
        """
        Initializes and returns the external database engine.
        
        :param db_creds: Dictionary containing the external database credentials.
        :return: Initialized database engine.
        """
        return self._create_engine(
            db_creds["RDS_HOST"],
            db_creds["RDS_PORT"],
            db_creds["RDS_DATABASE"],
            db_creds["RDS_USER"],
            db_creds["RDS_PASSWORD"]
        )

    def _init_local_db_engine(self, db_creds: dict) -> 'Engine':
        """
        Initializes and returns the internal database engine.
        
        :param db_creds: Dictionary containing the internal database credentials.
        :return: Initialized database engine.
        """
        return self._create_engine(
            db_creds["server"],
            db_creds["port"],
            db_creds["database"],
            db_creds["username"],
            db_creds["password"]
        )

    def _create_engine(self, host: str, port: str, db: str, user: str, password: str) -> 'Engine':
        """
        Creates and returns a database engine.
        
        :param host: Database host address.
        :param port: Database port number.
        :param db: Name of the database.
        :param user: Username for the database.
        :param password: Password for the database.
        :return: Database engine.
        """
        return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

    def _list_rds_table(self, db_engine: 'Engine') -> tuple:
        """
        Extracts and returns the table names from the given database engine.
        
        :param db_engine: Initialized database engine.
        :return: Tuple containing the list of table names, user table name, and order table name.
        """
        inspector = inspect(db_engine)
        table_names = inspector.get_table_names()
        user_table_name = next((table for table in table_names if 'users' in table.lower()), None)
        order_table_name = next((table for table in table_names if 'orders' in table.lower()), None)
        return table_names, user_table_name, order_table_name

    def upload_to_db(self, df, table_name: str) -> None:
        """
        Uploads a DataFrame to the specified table in the internal database.
        
        :param df: DataFrame to upload.
        :param table_name: Name of the table in which the data should be uploaded.
        """
        df.to_sql(table_name, self.internal_db_engine, if_exists="replace", index=False)
        self.internal_db_engine.execute("COMMIT;")
        print(f"Successfully uploaded {table_name} to the database!")

    def _test_connection(self, engine: 'Engine') -> None:
        """
        Tests the connection to the given database engine.
        
        :param engine: Initialized database engine.
        """
        try:
            with engine.connect() as connection:
                print("Successfully connected!")
        except Exception as e:
            print(f"Failed to connect. Error: {e}")
