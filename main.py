from Database import *

db=Database()
db.connect()
db.ext_to_file()
db.connect_to_olap_stg_db()  # Connect to the external database

# Fetch the table names from the external database
tables = db.fetch_table_names_from_stg_db()

# If tables are fetched, load data for each table
if tables:
    for table in tables:
        table_name = table[0]  # Extract table name from the tuple
        file_name = f"{table_name}.csv"  # Assuming CSV filenames are the same as table names
        db.load_to_table(table_name, file_name)
# # data = db.fetch("SELECT * FROM Customer;")
db.disconnect()