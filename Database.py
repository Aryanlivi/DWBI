import psycopg2
from Variables import *
from Loggers import *

logger=Logger().get_logger()
class Database():
    def __init__(self):
        self.cursor=None
        self.connection=None   
    def connect(self):
        try:
            self.connection=psycopg2.connect(
                host=Variables('server').get_variable(),
                database=Variables('database').get_variable(),
                user=Variables('username').get_variable(),
                password=Variables('password').get_variable()
            )
            logger.info("Database connection established successfully.")
            return self.connection 
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
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