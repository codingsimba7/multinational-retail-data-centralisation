import yaml
from sqlalchemy import create_engine, inspect


class DatabaseConnector():
    ## define initialization function, which gives us the database credentials and the database engine, the database tables and the user table name
    ## which is essential for inherting the class in other python scripts
    def __init__(self,external_database_file="db_creds.yaml",internal_database_file="local_db_creds.yaml"):
        ## instance will now have the database credentials and the database engine as well as the database tables and the user table name 
        ## in other words this is calling the other functions in the class and as a result the instance will have all the necessary attributes
        self.external_db_creds=self.read_db_creds(external_database_file)
        self.external_db_engine=self.init_db_engine(self.external_db_creds)
        self.rds_tables,self.user_table_name,self.orders_table_name=self.list_rds_table(self.external_db_engine)
        self.internal_db_creds=self.read_db_creds(internal_database_file)
        self.internal_db_engine=self.init_local_db_engine(self.internal_db_creds)

    ## read the database credentials from the yaml file
    def read_db_creds(self,file):
        with open(file) as f:
            db_creds = yaml.load(f, Loader=yaml.FullLoader)
        return db_creds
    ## initialize the database engine
    def init_db_engine(self,db_creds):
        db_host=db_creds["RDS_HOST"]
        db_password=db_creds["RDS_PASSWORD"]
        user=db_creds["RDS_USER"]
        db=db_creds["RDS_DATABASE"]
        db_port=db_creds["RDS_PORT"]
        db_engine = create_engine(f"postgresql+psycopg2://{user}:{db_password}@{db_host}:{db_port}/{db}")

        return db_engine
    def init_local_db_engine(self,db_creds):
        db_host=db_creds["server"]
        db_port=db_creds["port"]
        db=db_creds["database"]
        db_user=db_creds["username"]
        db_password=db_creds["password"]

        db_engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db}")
        return db_engine
    ## extract the database tables and the user table name
    def list_rds_table(self,db_engine):
        inspector = inspect(db_engine)
        table_names=inspector.get_table_names()
        user_table_name= next((table for table in table_names if 'users' in table.lower()), None)
        order_table_name= next((table for table in table_names if 'orders' in table.lower()), None)
        return table_names,user_table_name,order_table_name
    
    def local_db_engine(self,db_creds):
        db_host=db_creds["LOCAL_HOST"]
        db_password=db_creds["LOCAL_PASSWORD"]
        user=db_creds["LOCAL_USER"]
        db=db_creds["LOCAL_DATABASE"]
        db_port=db_creds["LOCAL_PORT"]
        db_engine = create_engine(f"postgresql+psycopg2://{user}:{db_password}@{db_host}:{db_port}/{db}")

        return db_engine
    
    def upload_to_db(self,df,table_name):
        df.to_sql(
            table_name,
            self.internal_db_engine,
            if_exists="replace",
            index=False)
        self.internal_db_engine.execute("COMMIT;")
        print(f"Successfully uploaded {table_name} to the database!")

    def test_connection(self, engine):
        try:
            connection = engine.connect()
            print("Successfully connected!")
            connection.close()
        except Exception as e:
            print(f"Failed to connect. Error: {e}")





    



    
    