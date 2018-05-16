
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
    cnx.cursor().execute("truncate table tbcustomersummary")
with fuckit:
    cnx.cursor().execute("create or replace table tbcustomersummary as (select * from vwcustomersummary)")
with fuckit:
    cnx.cursor().execute("truncate table tbtopcustomer")
with fuckit:
    cnx.cursor().execute("create or replace table tbtopcustomer as (select * from vwtopcustomer)")
with fuckit:
    cnx.cursor().execute("truncate table tbnewcustomer")
with fuckit:
    cnx.cursor().execute("create or replace table tbnewcustomer as (select * from vwnewcustomer)")
with fuckit:
    cnx.cursor().execute("truncate table tbtransaction")
with fuckit:
    cnx.cursor().execute("create or replace table tbtransaction as (select * from vwtransaction)")
with fuckit:
    cnx.cursor().execute("truncate table tbcustomer")
with fuckit:
    cnx.cursor().execute("create or replace table tbcustomer as (select * from vwcustomer)")
with fuckit:
    cnx.cursor().execute("truncate table tbmastercustomer")
with fuckit:
    cnx.cursor().execute("create or replace table tbmastercustomer as (select * from vwmastercustomer)")
with fuckit:
    cnx.cursor().execute("truncate table tblink")
with fuckit:
    cnx.cursor().execute("create or replace table tblink as (select * from vwlink)")
with fuckit:
    cnx.cursor().execute("grant select on all tables in schema PRODUCTION.PUBLIC to role looker_role")

