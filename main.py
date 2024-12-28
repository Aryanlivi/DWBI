from Database import *

db=Database()
db.connect()
# db.execute_query(
#     '''
#     CREATE TABLE Customer (
#     CustomerID SERIAL PRIMARY KEY,          
#     FirstName VARCHAR(50) NOT NULL,         
#     LastName VARCHAR(50) NOT NULL,          
#     PhoneNumber VARCHAR(15),                
#     Address TEXT                            
#     );
#     '''
# )

data = db.fetch("SELECT * FROM Customer;")
db.disconnect()