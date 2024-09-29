import pandas as pd
import pytest
from sqlalchemy import create_engine
import cx_Oracle

mysql_conn=create_engine('mysql+pymysql://root:Sur_nar26@localhost:3306/etlretailproject')
orcl_conn=create_engine('oracle+cx_oracle://system:admin@localhost:1521/xe')

def test_sales_data_extract():
    expected_df=pd.read_csv("Input_Data/sales_data.csv")
    query="select * from sales_staging"
    actual_df=pd.read_sql(query,mysql_conn)
    assert actual_df.equals(expected_df),f"Data mismatch between actual and expected sales_data. Please verify..."

def test_product_data_extract():
    expected_df=pd.read_csv("Input_Data/product_data.csv")
    query="select * from product_staging"
    actual_df=pd.read_sql(query,mysql_conn)
    assert actual_df.equals(expected_df),f"Data mismatch between actual and expected product_data. Please verify..."

def test_inventory_data_extract():
    expected_df=pd.read_xml("Input_Data/inventory_data.xml")
    query="select * from inventory_staging"
    actual_df=pd.read_sql(query,mysql_conn)
    assert actual_df.equals(expected_df),f"Data mismatch between actual and expected inventory_data. Please verify..."

def test_supplier_data_extract():
    expected_df=pd.read_json("Input_Data/supplier_data.json") 
    query="select * from supplier_staging"
    actual_df=pd.read_sql(query,mysql_conn)
    assert actual_df.equals(expected_df),f"Data mismatch between actual and expected supplier_data. Please verify..."

def test_store_data_extract():
    quer_orcl="select * from store"
    expected_df=pd.read_sql(quer_orcl,orcl_conn)
    quer_mysql="select * from store_staging"
    actual_result=pd.read_sql(quer_mysql,mysql_conn)
    assert actual_result.equals(expected_df),f"Data mismatch between actual and expected supplier_data. Please verify..."