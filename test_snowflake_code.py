
import pandas as pd
import pytest
import contextlib
import snowflake.connector

# Replace these values with your actual Snowflake account details
@contextlib.contextmanager
def snowflake_connection():
    conn = snowflake.connector.connect(
	user='Anshumala',
	password='9158354832Aman#7512',
	account='OKXVYKL-PH26326',  # e.g. 'xy12345.us-east-1'
	warehouse='COMPUTE_WH',
	database='SNOWFLAKE_LEARNING_DB',
	schema='PUBLIC'
)
    try:
        yield conn
    finally:
        conn.close()
def test_record_count_check():
    with snowflake_connection() as conn:
        query = "SELECT count(*) FROM PROD"
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        print(result[0])
        assert result[0] == 4, "Record count does not match expected value"
        cursor.close()

def test_column_existence_check():
    with snowflake_connection() as conn:
        query = "Select column_name from information_schema.columns where table_name='PROD'"
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [row[0] for row in cursor.fetchall()]  # Assuming the column name is in the 3rd position
        expected_columns = {'ID', 'PRODUCT_NAME', 'PRICE', 'PRODUCER_ID', 'DEPT_ID'}
        assert expected_columns.issubset(set(columns)), "Not all expected columns are present"
        cursor.close()
      

def test_datatypes_check():
    with snowflake_connection() as conn:
        queryy="Select Data_Type from information_schema.columns where table_name='PROD'"
        cursor = conn.cursor()
        cursor.execute(queryy)
        datatypes = [row[0] for row in cursor.fetchall()]
        expected_datatypes = ['TEXT', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER']
        assert datatypes== expected_datatypes, "Data types do not match expected values"
        cursor.close()

def test_null_values_check():
    with snowflake_connection() as coon:
        query="Select is_nullable from information_schema.columns where table_name='PROD'"
        cursor=coon.cursor()
        cursor.execute(query)
        is_null=[row[0] for row in cursor.fetchall()]
        expected_null = ["YES","YES","YES","YES","YES"]
        assert is_null==expected_null,'null validation fail'

def test_null_valus2and 3():


xcfvsfdgsfdsgit

V6