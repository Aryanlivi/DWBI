from Database import *
from Variables import SRC_DB,STG_DB,SRC_CSV_OP_PATH

src_db_instance=Database(database=SRC_DB)
src_db_instance.ext_to_file()

stg_db_instance=Database(database=STG_DB)
stg_db_instance.connect(host=host)

src_tables =src_db_instance.export_tables_to_csv(SRC_CSV_OP_PATH)

# if tables:
#     for table in tables:
#         table_name = table[0]  # Extract table name from the tuple
#         file_name = f"{table_name}.csv"  # Assuming CSV filenames are the same as table names
#         stg_db_instance.load_csv_to_table(table_name, file_name)
    
# # data = db.fetch("SELECT * FROM Customer;")
db.disconnect()