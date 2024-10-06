import pandas as pd
import pytest
from sqlalchemy import create_engine
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

@pytest.mark.regression
def test_fact_sales_table_validation():
    try:
        logger.info(f"fact_sales table loading started and is in progress")
        query_expected="""select sales_id, product_id,store_id,quantity,total_sales,sale_date
               from sales_details """
        query_actual=""" select * from fact_sales """
        db_to_db_validate(query_expected,conn_mysql,query_actual,conn_mysql)
        logger.info(f"Loading Completed successfully for the table fact_sales")
    except Exception as e:
        logger.error(f"Error while processing the table fact_sales. Verify once..{e}")

@pytest.mark.regression
def test_fact_inventory_table_validation():
    try:
        logger.info(f"fact_inventory table loading started and is in progress")
        query_expected=""" select product_id,store_id,quantity_on_hand,last_updated
               from inventory_staging """            
        query_actual=""" select * from fact_inventory """
        db_to_db_validate(query_expected,conn_mysql,query_actual,conn_mysql)
        logger.info(f"Loading Completed successfully for the table fact_inventory ")
    except Exception as e:
        logger.error(f"Error while processing the table fact_inventory. Verify once..{e}")    

@pytest.mark.regression
def test_monthly_sales_summary_table_validation():
    try:
        logger.info(f"monthly_sales_summary table loading started and is in progress")
        query_expected=""" select product_id,month,year,total_sales
               from monthly_sales_aggregator """
        query_actual=""" select * from monthly_sales_summary """
        db_to_db_validate(query_expected,conn_mysql,query_actual,conn_mysql)
        logger.info(f"Loading Completed successfully for the table monthly_sales_summary")
    except Exception as e:
        logger.error(f"Error while processing the table monthly_sales_summary. Verify once..{e}")

@pytest.mark.regression
def test_inventory_levels_by_store_table_validation():
    try:
        logger.info(f"inventory_levels_by_store table loading started and is in progress ")
        query_expected=""" select store_id,total_inventory
               from aggregated_inventory_data; """
        query_actual=""" select * from inventory_levels_by_store """
        db_to_db_validate(query_expected,conn_mysql,query_actual,conn_mysql)
        logger.info(f"Loading Completed successfully for the table inventory_levels_by_store")
    except Exception as e:
        logger.error(f"Error while processing the table inventory_levels_by_store. Verify once..{e}")    

