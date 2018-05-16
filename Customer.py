#!/usr/bin/env python

import snowflake.connector
import fuckit
import keyring
PASSWORD = keyring.get_password("Snowflake" , "PYTHON_SERVICE")

# this creates a log file for the script
import logging
logging.basicConfig(
    filename='C:/mystuff/tmp/snowflake_python_connector.log',
    level=logging.INFO)

# set account information
ACCOUNT = 'lillypulitzer'
USER = 'PYTHON_SERVICE'
PASSWORD = keyring.get_password("Snowflake" , "PYTHON_SERVICE")
WAREHOUSE = 'STITCH_TEST'
DATABASE = 'staging'
SCHEMA = 'Python_production'

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
with fuckit:
    cnx.cursor().execute("create or replace stage Raw_transaction copy_options=(on_error='skip_file')")
with fuckit:
    cnx.cursor().execute("create or replace stage Raw_mastercustomer copy_options=(on_error='skip_file')")
with fuckit:
    cnx.cursor().execute("create or replace stage Raw_customer copy_options=(on_error='skip_file')")
with fuckit:
    cnx.cursor().execute("create or replace stage Raw_customersummary copy_options=(on_error='skip_file')")

# load data from CSV file into pre-made Snowflake table
with fuckit:
    cnx.cursor().execute("PUT file://K:/InfoDepth/ARCHIVE/RCVAgilOne/A1_DataExport_Prod_transaction*.csv @Raw_transaction")
with fuckit:
    cnx.cursor().execute("Truncate table Raw_transaction")
with fuckit:
    cnx.cursor().execute("COPY INTO Raw_transaction from @Raw_transaction file_format = (format_name='Raw_customer')")
with fuckit:
    cnx.cursor().execute("PUT file://K:/InfoDepth/ARCHIVE/RCVAgilOne/A1_DataExport_Prod_mastercustomer*.csv @Raw_mastercustomer")
with fuckit:
    cnx.cursor().execute("Truncate table Raw_mastercustomer")
with fuckit:
    cnx.cursor().execute("COPY INTO Raw_mastercustomer from @Raw_mastercustomer file_format = (format_name='Raw_customer')")
with fuckit:
    cnx.cursor().execute("PUT file://K:/InfoDepth/ARCHIVE/RCVAgilOne/A1_DataExport_Prod_customer.csv @Raw_customer")
with fuckit:
    cnx.cursor().execute("COPY INTO Raw_customer from @Raw_customer file_format = (format_name='Raw_customer')")
with fuckit:
    cnx.cursor().execute("PUT file://K:/InfoDepth/ARCHIVE/RCVAgilOne/A1_DataExport_Prod_customer_*.csv @Raw_customer")
with fuckit:
    cnx.cursor().execute("Truncate table Raw_customer")
with fuckit:
    cnx.cursor().execute("COPY INTO Raw_customer from @Raw_customer file_format = (format_name='Raw_customer')")
with fuckit:
    cnx.cursor().execute("PUT file://K:/InfoDepth/ARCHIVE/RCVAgilOne/A1_DataExport_Prod_customersummary*.csv @Raw_customersummary")
with fuckit:
    cnx.cursor().execute("Truncate table Raw_customersummary ")
with fuckit:
    cnx.cursor().execute("COPY INTO Raw_customersummary from @Raw_customersummary file_format = (format_name='Raw_customer')")

                                                                                                             
 #Querying data
#cur = cnx.cursor()
#try:
 #   cur.execute("SELECT * FROM max_test_transaction limit 10")
  #  for (col1, col2, col3, col4, col5, col6) in cur:
   #   print('{0}','{1}','{2}','{3}','{4}','{5}'.format(col1, col2, col3, col4, col5, col6))
#finally:
 #   cur.close()

#for (transaction_sourcecustomernumber, transaction_customerid, transaction_sourceorganizationnumber, transaction_sourcetransactionnumber, transaction_id, execution_id) in cnx.cursor().execute("SELECT * FROM max_test_transaction limit 10"):
#    print('{0}','{1}','{2}','{3}','{4}','{5}'.format(transaction_sourcecustomernumber, transaction_customerid, transaction_sourceorganizationnumber, transaction_sourcetransactionnumber, transaction_id, execution_id))
