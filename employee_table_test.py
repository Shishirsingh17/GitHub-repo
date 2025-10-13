import pandas as pd
import snowflake.connector

def snowflake_connection():
    conn=snowflake.connector.connect(
        user='Anshumala',
	password='9158354832Aman#7512',
	account='OKXVYKL-PH26326',  # e.g. 'xy12345.us-east-1'
	warehouse='COMPUTE_WH',
	database='SNOWFLAKE_LEARNING_DB',
	schema='PUBLIC'
    )
    return conn


def test_record_count():
    with snowflake_connection() as conn:
        querry='Select count(*) from employees'
        cursor=conn.cursor()
        cursor.execute(querry)
        result=cursor.fetchone()
        expected_result=9
        assert result[0]==expected_result,'record count not matching'


def test_column_check():
    with snowflake_connection() as con:
        querry="select column_name from information_schema.columns where table_name='EMPLOYEES' order by column_name"
        cursor=con.cursor()
        cursor.execute(querry)
        result=[row[0] for row in cursor.fetchall()]
        expected_result=['DEPARTMENT', 'ID', 'MANAGERID', 'NAME']
        assert result==expected_result,'column names not matching'

def test_datatype_check():
     with snowflake_connection() as conn:
          query="select data_type from information_schema.columns where table_name='EMPLOYEES' order by column_name"
          cursor=conn.cursor()
          cursor.execute(query)
          result=[row[0] for row in cursor.fetchall()]
          expected=['TEXT','NUMBER','NUMBER','TEXT']
          assert result==expected,'Datatype is not matching'

def test_nullable_column():
    with snowflake_connection() as conn:
        query="select is_nullable from information_schema.columns where table_name='EMPLOYEES' order by column_name"
        cursor=conn.cursor()
        cursor=cursor.execute(query)
        result=[row[0] for row in cursor.fetchall()]
        expected=['YES','NO','YES','YES']
        assert result==expected,'null column is not matching'
        
def test_duplicate_check():
    with snowflake_connection() as conn:
        query='select id,count(*) from employees group by id having count(*)>1'
        cursor=conn.cursor()
        cursor.execute(query)
        result=[row[0] for row in cursor.fetchall() ]
        assert len(result)==0, 'Table having duplicate'

"""
querry="select id,count(*) from employees group by id having count(*)>1"
cursor=snowflake_connection().cursor()
cursor.execute(querry)
result=cursor.fetchall()
print(len(result))
"""