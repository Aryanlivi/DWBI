import json
class Variables:
    def __init__(self):
        pass
    @staticmethod   
    def get_variable(variable_name):
        path="config/config.cfg"
        try:
            with open(path,"r") as file:
                file_content=file.read()
                file_content=json.loads(file_content)
                
                return file_content[variable_name] 
                # print(file_content)
        except Exception as e:
            print(f"Error:{e}")
        
HOST=Variables.get_variable('server')
USER=Variables.get_variable('username')
PASSWORD=Variables.get_variable('password')
SRC_DB=Variables.get_variable('src_db') 
LOG_PATH=Variables.get_variable('log_path')
LOG_NAME=Variables.get_variable('log_name')
STG_CSV_OP_PATH=Variables.get_variable('stg_csv_output_path')
SRC_CSV_OP_PATH=Variables.get_variable('src_csv_output_path')
STG_DB=Variables.get_variable('STG_DB')