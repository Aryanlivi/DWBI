import json
class Variables:
    def __init__(self,variable_name):
        self.path="D:\Learning\DW_BI_PROJECT\config\config.cfg"
        self.name=variable_name
        
    def get_variable(self):
        with open(self.path,"r") as file:
            file_content=file.read()
            file_content=json.loads(file_content)
            
            return file_content[self.name]
            # print(file_content)
            

var=Variables('database')
print(var.get_variable())