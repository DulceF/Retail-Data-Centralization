from sqlalchemy import create_engine 
import yaml
import pandas as pd

#(Milestone2, Task2, Step2)
#This class is used to connect and upload data to the database

class DatabaseConector:
    def __init__(self, host, password, user,database,port):
        host = self.host
        port = self.password
        user = self.user
        database = self.database
        port = self.port
        

