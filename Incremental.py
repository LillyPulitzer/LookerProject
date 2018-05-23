#!/usr/bin/env python
import snowflake.connector

# this creates a log file for the script
import logging
logging.basicConfig(
    filename='C:/mystuff/tmp/snowflake_python_connector.log',
    level=logging.INFO)

# set account information
ACCOUNT = 'lillypulitzer'
USER = 'PYTHON_SERVICE'
PASSWORD = 'Lilly1234'
WAREHOUSE = 'STITCH_TEST'
DATABASE = 'TESTING'
SCHEMA = 'Public'

# connect to Snowflake using default authenticator

cnx = snowflake.connector.connect(
user=USER,
password=PASSWORD,
account=ACCOUNT,
warehouse=WAREHOUSE,
database=DATABASE,
schema=SCHEMA,
)


# create stage for the put command

cnx.cursor().execute("create or replace stage raw_transaction copy_options=(on_error='skip_file')")

# load data from CSV file into pre-made Snowflake table

cnx.cursor().execute("PUT file://K:/InfoDepth/ARCHIVE/RCVAgilOne/A1_DataExport_Prod_transaction*.csv @Raw_transaction")
cnx.cursor().execute("Truncate table test_transaction")
cnx.cursor().execute("COPY INTO test_transaction from @Raw_transaction file_format = (format_name='Raw_customer')")
cnx.cursor().execute("merge into Raw_transaction s using( select transaction_sourcecustomernumber, transaction_customerid,transaction_sourcetransactionnumber, transaction_id, execution_id from test_transaction) as k on s.transaction_sourcetransactionnumber=k.transaction_sourcetransactionnumber when matched then update set s.transaction_sourcecustomernumber=k.transaction_sourcecustomernumber,s.transaction_customerid=k.transaction_customerid,s.transaction_sourcetransactionnumber=k.transaction_sourcetransactionnumber,s.transaction_id=k.transaction_id, s.execution_id=k.execution_id when not matched then insert (transaction_sourcecustomernumber, transaction_customerid, transaction_sourcetransactionnumber, transaction_id, execution_id) values (k.transaction_sourcecustomernumber, k.transaction_customerid,k.transaction_sourcetransactionnumber, k.transaction_id, k.execution_id)")

# Querying data
#cur = cnx.cursor()
#try:
#    cur.execute("SELECT * FROM max_test_python")
#    for (col1) in cur:
#       print('{0}'.format(col1))
#finally:
#    cur.close()
