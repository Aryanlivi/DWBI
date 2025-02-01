import psycopg2
from Variables import HOST,USER,PASSWORD,CSV_OP_PATH
import pandas as pd
from psycopg2 import sql
from Loggers import *

logger = Logger().get_logger()

class Database:
    def __init__(self,database):
        self.connection = None
        self.connect(database=database)
    
    def connect(self,database):
        """Establishes a database connection."""
        try:
            conn = psycopg2.connect(host=HOST, database=database, user=USER, password=PASSWORD)
            logger.info(f"Connected to database {database} successfully.")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to database {database}: {e}")
            return None
    def execute_query(self, conn, query, fetch=False):
        """Executes a query and optionally fetches results."""
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()
                logger.info(f"Query executed successfully: {query}")
                return cursor.fetchall() if fetch else None
        except Exception as e:
            logger.error(f"Error executing query '{query}': {e}")
            return None
    
    def fetch_table_names(self, conn):
        """Fetches table names from the database."""
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        return self.execute_query(conn, query, fetch=True)
    
    def export_tables_to_csv(self, conn, output_path):
        """Exports all tables to CSV files."""
        tables = self.fetch_table_names(conn) 
        if tables:
            for table in tables:
                table_name = table[0]
                query = f"SELECT * FROM {table_name};"
                data = self.execute_query(conn, query, fetch=True)
                if data:
                    df = pd.DataFrame(data, columns=[desc[0] for desc in conn.cursor().description])
                    df.to_csv(f"{output_path}/{table_name}.csv", index=False)
                    logger.info(f"Exported {table_name} to CSV.")
    
    def get_columns(self, conn, table_name):
        """Gets column names for a table."""
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
        columns = self.execute_query(conn, query, fetch=True)
        return [col[0] for col in columns] if columns else []

    def load_csv_to_table(self, conn, table_name, file_path):
        """Loads data from a CSV file into a PostgreSQL table using COPY."""
        try:
            columns = self.get_columns(conn, table_name)
            temp_table = f"temp_{table_name}"
            
            create_temp_query = f"CREATE TEMPORARY TABLE {temp_table} (LIKE {table_name} INCLUDING ALL);"
            self.execute_query(conn, create_temp_query)
            
            with open(file_path, 'r') as file:
                with conn.cursor() as cursor:
                    cursor.copy_expert(f"COPY {temp_table} FROM STDIN WITH CSV HEADER DELIMITER ','", file)
                    conn.commit()
            
            upsert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                SELECT {', '.join(columns)} FROM {temp_table}
                ON CONFLICT (id) DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in columns])};
            """
            self.execute_query(conn, upsert_query)
            logger.info(f"Data loaded successfully into '{table_name}'.")
        except Exception as e:
            logger.error(f"Error loading data into '{table_name}': {e}")
            conn.rollback()

    def disconnect(self, conn):
        """Closes a database connection."""
        if conn:
            conn.close()
            logger.info("Database connection closed.")
