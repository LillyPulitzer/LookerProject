#!/usr/bin/env python
import snowflake.connector
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
WAREHOUSE = 'LOOKER_WH'
SIZE = 'Large'


# connect to Snowflake using default authenticator
ctx = snowflake.connector.connect(
user=USER,
password=PASSWORD,
account=ACCOUNT,
warehouse=WAREHOUSE,
)

# execute
cs = ctx.cursor()
try:
    cs.execute("alter warehouse {} set warehouse_size = '{}'".format(WAREHOUSE,SIZE))
    one = cs.fetchone()
    print(one[0])
finally:
    cs.close()
