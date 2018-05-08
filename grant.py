
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
PASSWORD = 'Lilly1234'
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

cnx.cursor().execute("grant select on all tables in schema PRODUCTION.PUBLIC to role looker_role")
cnx.cursor().execute("grant select on all views in schema PRODUCTION.PUBLIC to role looker_role")


