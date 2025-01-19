import psycopg2
import Variables
from psycopg2 import sql
from Loggers import *
import pandas as pd

logger=Logger().get_logger()
class Database():
    def __init__(self):
        self.cursor=None
        self.connection=None   
        self.stg_connection=None
    def connect(self):
        try:
            self.connection=psycopg2.connect(
                host=Variables.host,
                database=Variables.database,
                user=Variables.user,
                password=Variables.password
            )
            logger.info("Database connection established successfully.")
            return self.connection 
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            return None
        
    def connect_to_olap_stg_db(self):
        try:
            # Connect to OLAP (target) database
            self.stg_connection = psycopg2.connect(
                host=Variables.host,
                database=Variables.STG_DB,
                user=Variables.user,
                password=Variables.password
            )
            logger.info("Connected to the OLAP STAGE database successfully.")
            return self.stg_connection
        except Exception as e:
            logger.error(f"Error connecting to the OLAP database: {e}")
            return None
        
    def execute_query(self,query):
        if self.connection:
            try:
                self.cursor = self.connection.cursor()
                self.cursor.execute(query)
                self.connection.commit()
                logger.info(f"Query executed successfully: {query}")
                self.cursor.close()
            except Exception as e:
                logger.error(f"Error executing query '{query}': {e}")
    
    def fetch_table_names_from_stg_db(self):
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        try:
            if self.stg_connection:
                self.cursor = self.stg_connection.cursor()
                self.cursor.execute(query)
                tables = self.cursor.fetchall()
                self.cursor.close()
                logger.info("Table names fetched from the external database successfully.")
                return tables
        except Exception as e:
            logger.error(f"Error fetching table names from the external database: {e}")
            return None

    def ext_to_file(self):
        query_table_names = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        try:
            tables = self.fetch(query_table_names)
            if tables:
                for table in tables:
                    table_name=table[0] 
                    
                    query_table_content=f"SELECT * FROM {table_name};"
                    content=self.fetch(query_table_content)
                    df = pd.DataFrame(content, columns=[desc[0] for desc in self.cursor.description])
                    df.to_csv(f"{Variables.csv_output_path}/{table_name}.csv",index=False)
        except Exception as e:
            logger.error(f"Error in ext_to_file:{e}")
            
    # def load_to_table(self, table_name, file_name): 
    #     # Use client-side `copy_expert` for CSV data loading
    #     try:
    #         if self.connection:
    #             with self.connection.cursor() as cursor:
    #                 file_path = f"{Variables.csv_output_path}/{file_name}"
    #                 with open(file_path, 'r') as file:
    #                     cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", file)
    #                 self.connection.commit()
    #                 logger.info(f"Data loaded successfully into '{table_name}'.")
    #     except Exception as err:
    #         logger.error(f"Error loading data into '{table_name}': {err}")
    #         self.connection.rollback()
    
    def get_columns_for_table(self, table_name):
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            columns = cursor.fetchall()
            return [column[0] for column in columns]
    
    def load_to_table(self, table_name, file_name):
        try:
            if self.stg_connection:
                with self.stg_connection.cursor() as cursor:
                    file_path = f"{Variables.csv_output_path}/{file_name}"

                    # Step 1: Load data into a temporary table
                    temp_table_name = f"temp_{table_name}"

                    # Get actual columns for the table
                    columns = self.get_columns_for_table(table_name)
                    column_names = ', '.join(columns)  # Use dynamic columns

                    # Create a temporary table with the same structure as the target table
                    create_temp_table_query = f"CREATE TEMPORARY TABLE {temp_table_name} (LIKE {table_name} INCLUDING ALL);"
                    cursor.execute(create_temp_table_query)
                    self.stg_connection.commit()

                    # Step 2: Load data from the CSV file into the temporary table
                    with open(file_path, 'r') as file:
                        cursor.copy_expert(f"COPY {temp_table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", file)

                    # Verify data in the temporary table
                    cursor.execute(f"SELECT COUNT(*) FROM {temp_table_name};")
                    row_count = cursor.fetchone()
                    logger.info(f"Rows in temporary table {temp_table_name}: {row_count[0]}")

                    # Step 3: Perform an UPSERT operation to load data from the temp table
                    upsert_query = f"""
                        INSERT INTO {table_name} ({column_names})  
                        SELECT {column_names}
                        FROM {temp_table_name}
                        ON CONFLICT (id)  -- Replace with your primary key column
                        DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in columns])};  -- dynamically update columns
                    """
                    logger.info(f"Executing upsert query: {upsert_query}")
                    cursor.execute(upsert_query)
                    self.stg_connection.commit()
                    logger.info(f"Data loaded successfully into '{table_name}' with conflict handling.")
        except Exception as err:
            logger.error(f"Error loading data into '{table_name}': {err}")
            self.stg_connection.rollback()





    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed.")
        
    def fetch(self,query):
        try:
            if self.connection:
                self.cursor = self.connection.cursor()
                self.cursor.execute(query)
                result = self.cursor.fetchall()
                logger.info(f"Data fetched successfully for query: {query}")
                self.cursor.close()
                logger.info(f"Fetched Data:{result}")
                return result
        except Exception as e:
            logger.error(f"Error fetching data for query '{query}': {e}")
            return None 