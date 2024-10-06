import pandas as pd
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import logging
from CommonUtilities.config import *
from CommonUtilities.utilities import *

# Set up logging configuration
logging.basicConfig(
    filename='Logs/ETLPipeline.log',  # Name of the log file
    filemode='a',        # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO    # Set the logging level
)
logger = logging.getLogger(__name__)

#conn_mysql=create_engine('mysql+pymysql://root:Sur_nar26@localhost:3306/etlretailproject')
conn_mysql = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

#conn_oracle=create_engine("oracle+cx_oracle://system:admin@localhost:1521/xe")
conn_oracle = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

@pytest.mark.smoke
def test_dataextract_from_salesdatafile():
    try:
        logger.info(f"Starting to load data from CSV file to sales_data into staging ")
        file_to_db_validate("Input_Data/sales_data.csv","sales_staging",conn_mysql,"csv")
        logger.info(f"Completed loading sales_data into staging ")
    except Exception as e:    
        logger.error(f"Data extraction failed from Source to sales_staging. Please verify..{e}")

@pytest.mark.smoke     
def test_dataextract_from_productdatafile():
    try:
        logger.info(f"Starting to load data from CSV file to product_data into staging")
        file_to_db_validate("Input_Data/product_data.csv","product_staging",conn_mysql,"csv")
        logger.info(f"Completed loading product_data into staging")
    except Exception as e:
         logger.error(f"Data extraction failed from Source to product_staging. Please verify..{e}")   

@pytest.mark.smoke    
def test_dataextract_from_inventorydatafile():
    try:
        logger.info(f"Starting to load data from XML file to inventory_data into staging")
        file_to_db_validate("Input_Data/inventory_data.xml","inventory_staging",conn_mysql,"xml")
        logger.info(f"Completed loading inventory_data into staging")
    except Exception as e:
        logger.error(f"Data extraction failed from Source to inventory_staging. Please verify..{e}")

@pytest.mark.smoke
def test_dataextract_from_supplierdatafile():
    try:
        logger.info(f"Starting to load data from JSON file to supplier_data into staging")
        file_to_db_validate("Input_Data/supplier_data.json","supplier_staging",conn_mysql,"json")
        logger.info(f"Completed loading supplier_data into staging")
    except Exception as e:
        logger.error(f"Data extraction failed from Source to supplier_staging. Please verify..{e}")

@pytest.mark.smoke
def test_dataextract_store_oracledata():
    try:
        logger.info(f"Starting to load source data from Oracle db to store_staging into staging")
        query1="""select * from store"""
        query2="""select * from store_staging"""
        db_to_db_validate(query1,conn_oracle,query2,conn_mysql)
        logger.info(f"Completed loading store_data into staging")
    except Exception as e:
        logger.error(f"Data extraction failed from Source to store_staging. Please verify..{e}")