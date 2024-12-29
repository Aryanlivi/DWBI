import json
class Variables:
    def __init__(self):
        pass
    @staticmethod   
    def get_variable(variable_name):
        path="D:\Learning\DW_BI_PROJECT\config\config.cfg"
        try:
            with open(path,"r") as file:
                file_content=file.read()
                file_content=json.loads(file_content)
                
                return file_content[variable_name]
                # print(file_content)
        except Exception as e:
            print(f"Error:{e}")
        
host=Variables.get_variable('server')
database=Variables.get_variable('database')
user=Variables.get_variable('username')
password=Variables.get_variable('password')
log_path=Variables.get_variable('log_path')
log_name=Variables.get_variable('log_name')