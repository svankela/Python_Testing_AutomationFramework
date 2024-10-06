import pandas as pd
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import logging
from CommonUtilities.config import *
import os

# Set up logging configuration
logging.basicConfig(
    filename='Logs/etlprocess.log',  # Name of the log file
    filemode='a',        # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO    # Set the logging level
)
logger = logging.getLogger(__name__)

#conn_mysql=create_engine('mysql+pymysql://root:Sur_nar26@localhost:3306/etlretailproject')
conn_mysql = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

#conn_oracle=create_engine("oracle+cx_oracle://system:admin@localhost:1521/xe")
conn_oracle = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

def file_to_db_validate(file_path,table_name,db_engine,file_type):
    if file_type=='csv':  
        expected_df=pd.read_csv(file_path)
    elif  file_type=='json':
        expected_df=pd.read_json(file_path)
    elif  file_type=='xml':
        expected_df=pd.read_xml(file_path,xpath='.//item')     
    else:
        raise ValueError(f"Unsupported file type:{file_type} ") 

    query = f"Select * from {table_name};"
    actual_df=pd.read_sql(query,db_engine)
    assert actual_df.equals(expected_df),f"Data mismatch between the file and table. Please verify.."

def db_to_db_validate(query1,db_engine1,query2,db_engine2):
    actual_df=pd.read_sql(query1,db_engine1).astype(str) # Data type between source file and python varies and data type conversion is essential in order to match the data
    expected_df=pd.read_sql(query2,db_engine2).astype(str) # Data type between source file and python varies and data type conversion is essential in order to match the data
    assert actual_df.equals(expected_df),f"Data mismatch between db and table. Please verify.." 

# Negative test cases

#check file existing in the mentioned path or not
def check_file_existence(file_path):
    return os.path.isfile(file_path)    

#check if there is empty file in the mentioned path
def check_file_empty(file_path):
    if (os.path.getsize(file_path==0)):
        return True
    else:
        return False