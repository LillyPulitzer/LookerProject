
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
with fuckit:
    cnx.cursor().execute("drop table tbdate")
with fuckit:
    cnx.cursor().execute("create or replace table tbdate as (select * from vwdate)")
with fuckit:
    cnx.cursor().execute("drop table tbstore")
with fuckit:
    cnx.cursor().execute("create or replace table tbstore as (select * from vwstore)")
with fuckit:
    cnx.cursor().execute("drop table tbproduct")
with fuckit:
    cnx.cursor().execute("create or replace table tbproduct as (select * from vwproduct)")
with fuckit:
    cnx.cursor().execute("drop table tbsalesplan")
with fuckit:
    cnx.cursor().execute("create or replace table tbsalesplan as (select * from vwsalesplan)")
with fuckit:
    cnx.cursor().execute("drop table tbonhand")
with fuckit:
    cnx.cursor().execute("create or replace table tbonhand as (select * from vwonhand)")
with fuckit:
    cnx.cursor().execute("drop table TBONHAND_FOR_ST_EXPLORE")
with fuckit:
    cnx.cursor().execute("create or replace table TBONHAND_FOR_ST_EXPLORE as (select * from VWONHAND_FOR_ST_EXPLORE)")
with fuckit:
    cnx.cursor().execute("drop table tbdistributions")
with fuckit:
    cnx.cursor().execute("create or replace table tbdistributions as (select * from vwdistributions)")
with fuckit:
    cnx.cursor().execute("drop table tbsalesplanweekly")
with fuckit:
    cnx.cursor().execute("create or replace table tbsalesplanweekly as (select * from vwsalesplanweekly)")
with fuckit:    
    cnx.cursor().execute("grant select on all tables in schema PRODUCTION.PUBLIC to role looker_role")
with fuckit:
    cnx.cursor().execute("drop table TBSTORETEXTS")
with fuckit:
    cnx.cursor().execute("create or replace table TBSTORETEXTS as (select * from VWSTORETEXTS)")
with fuckit:
    cnx.cursor().execute("grant select on all tables in schema PRODUCTION.PUBLIC to role looker_role")


