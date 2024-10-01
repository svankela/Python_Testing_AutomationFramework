import pandas as pd
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import logging
from CommonUtilities.config import *
from CommonUtilities.utilities import *

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

def test_dataextract_from_salesdatafile():
    file_to_db_validate("Input_Data/sales_data.csv","sales_staging",conn_mysql,"csv")
    
def test_dataextract_from_productdatafile():
    file_to_db_validate("Input_Data/product_data.csv","product_staging",conn_mysql,"csv")
    
def test_dataextract_from_inventorydatafile():
    file_to_db_validate("Input_Data/inventory_data.xml","inventory_staging",conn_mysql,"xml")

def test_dataextract_from_supplierdatafile():
    file_to_db_validate("Input_Data/supplier_data.json","supplier_staging",conn_mysql,"json")

def test_dataextract_store_oracledata():
    query1="""select * from store"""
    query2="""select * from store_staging"""
    db_to_db_validate(query1,conn_oracle,query2,conn_mysql)