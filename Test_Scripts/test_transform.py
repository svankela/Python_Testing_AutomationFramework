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
def test_filter_transformation_validation():
    try:
        logger.info(f"Filter transformation loading in progress")
        query_expected=""" select * from sales_staging where sale_date >= '2024-09-05' """
        query_actual=""" select * from sales_filter """
        db_to_db_validate(query_expected,conn_mysql,query_actual,conn_mysql)
        logger.info(f"Loading Completed successfully for Filter Transformation into sales_filter")
    except Exception as e:
        logger.error(f"Error while processing sales_filter transformation. Verify once..{e}")

@pytest.mark.regression
def test_router_low_sales_transformation_validation():
    try:
        logger.info(f"Router transformation loading for low_sales in progress")
        query_expected = """ select * from sales_filter where region='low' """
        query_actual = """ select * from sales_low """
        db_to_db_validate(query_expected,conn_mysql,query_actual,conn_mysql)
        logger.info(f"Loading completed successfully for low_sales Router transformation")
    except Exception as e:
        logger.error(f"Error while processing low_sales router transformation. Verify once..{e}")    

@pytest.mark.regression
def test_high_sales_router_transformation_validation():
    try:
        logger.info(f"Router transformation loading for high_sales in progress")
        query_expected=""" select * from sales_filter where region = 'High' """
        query_actual=""" select * from sales_high """
        db_to_db_validate(query_expected,conn_mysql,query_actual,conn_mysql)
        logger.info(f"Loading completed successfully for low_sales Router transformation")
    except Exception as e:
        logger.error(f"Error while processing high_sales router transformation. Verify once..{e}")

@pytest.mark.regression
def test_aggregate_transformation_validation():
    try:
        logger.info(f"Aggregate transformation loading for monthly_sales_aggregator in progress")
        query_expected=""" select product_id,month(sale_date) as month,year(sale_date) as year,
                       sum(quantity*price) as total_sales from sales_filter
                       group by product_id,month(sale_date),year(sale_date) """        
        query_actual=""" select * from monthly_sales_aggregator """
        db_to_db_validate(query_expected,conn_mysql,query_actual,conn_mysql)
        logger.info(f"Loading completed successfully for monthly_sales_aggregator transformation")
    except Exception as e:
        logger.error(f"Error while processing monthly_sales_aggregator transformation. Verify once..{e}")

@pytest.mark.regression
def test_joiner_transformation_validation():
    try:
        logger.info(f"Joiner transformation loading for sales_details in progress ")
        query_expected=""" SELECT ss.sales_id, ss.product_id, ps.product_name, ss.store_id, st.store_name, 
        ss.quantity, ss.price, (ss.quantity*ss.price) as Total_Sales, ss.sale_date
        FROM sales_filter ss
        Inner join product_staging ps
        ON ss.product_id=ps.product_id
        Inner join store_staging st
        ON ss.store_id=st.store_id """
        query_actual=""" select * from sales_details"""
        db_to_db_validate(query_expected,conn_mysql,query_actual,conn_mysql)
        logger.info(f"Loading completed successfully for sales_details transformation")
    except Exception as e:
        logger.error(f"Error while processing sales_details joiner transformation. Verify once..{e}")    

@pytest.mark.regression
def test_inventory_data_validation():
    try:
        logger.info(f"Inventory data loading started loading and is in progress")
        expected_query=""" Select store_id, sum(quantity_on_hand) as total_inventory
                           From inventory_staging
                           Group by store_id """
        actual_query=""" select * from aggregated_inventory_data """
        db_to_db_validate(expected_query,conn_mysql,actual_query,conn_mysql)
        logger.info(f"Loading completed successfully for inventory_data")
    except Exception as e:
        logger.info(f"Error while processing inventory_data. Verify once..{e}")    
