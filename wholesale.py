#!/usr/bin/env python
import snowflake.connector
import fuckit
import keyring
#PASSWORD = keyring.get_password("Snowflake" , "PYTHON_SERVICE")


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
DATABASE = 'production'
SCHEMA = 'public'

# connect to Snowflake using default authenticator

cnx = snowflake.connector.connect(
user=USER,
password=PASSWORD,
account=ACCOUNT,
warehouse=WAREHOUSE,
database=DATABASE,
schema=SCHEMA,
)

# truncate the table
with fuckit:
    cnx.cursor().execute("DROP TABLE TBWHOLESALE_CUSTOMER")
with fuckit:
    cnx.cursor().execute("CREATE TABLE TBWHOLESALE_CUSTOMER AS SELECT * FROM WHOLESALE_CUSTOMER")
with fuckit:
    cnx.cursor().execute("DROP TABLE TBWHOLESALE")
with fuckit:
    cnx.cursor().execute("CREATE TABLE TBWHOLESALE AS SELECT * FROM VWWHOLESALE")
with fuckit:
    cnx.cursor().execute("DROP TABLE TBPRODUCT_NEW_FINAL")
with fuckit:
    cnx.cursor().execute("CREATE TABLE TBPRODUCT_NEW_FINAL AS SELECT * FROM VWPRODUCT_NEW_FINAL")
with fuckit:
    cnx.cursor().execute("DROP TABLE TBDISTRO_RCPT_ADJUST")
with fuckit:
    cnx.cursor().execute("CREATE TABLE TBDISTRO_RCPT_ADJUST AS SELECT * FROM VWDISTRO_RCPT_ADJUST")
with fuckit:
    cnx.cursor().execute("DROP TABLE TBEUCLID_VISITORS")
with fuckit:
    cnx.cursor().execute("CREATE TABLE TBEUCLID_VISITORS AS SELECT * FROM VWEUCLID_VISITORS")
with fuckit:
    cnx.cursor().execute("grant select on all tables in schema PRODUCTION.PUBLIC to role looker_role")    
    
