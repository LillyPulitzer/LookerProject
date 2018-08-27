
#!/usr/bin/env python

import snowflake.connector
import fuckit
import keyring



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
    cnx.cursor().execute("Merge into tbstore as a using vwstore as b on a.store_key =b.store_key when matched then update set STORE_NAME=b.STORE_NAME,DISTRICT_NAME=b.DISTRICT_NAME,OPEN_OR_CLOSED=b.OPEN_OR_CLOSED,SQUARE_FOOTAGE=b.SQUARE_FOOTAGE,CHANNEL_NAME=b.CHANNEL_NAME,FP_OR_SALE_STORE=b.FP_OR_SALE_STORE,DISTRICT_SORT=b.DISTRICT_SORT,STORE_SORT=b.STORE_SORT,FULL_SORT=b.FULL_SORT,TRAFFIC_COUNTER=b.TRAFFIC_COUNTER,COMP_DATE=b.COMP_DATE,IS_COMP=b.IS_COMP,OPEN_DATE=b.OPEN_DATE when not matched then insert(STORE_KEY,STORE_NAME,DISTRICT_NAME,OPEN_OR_CLOSED,SQUARE_FOOTAGE,CHANNEL_NAME,FP_OR_SALE_STORE,DISTRICT_SORT,STORE_SORT,FULL_SORT,TRAFFIC_COUNTER,COMP_DATE,IS_COMP,OPEN_DATE) values (b. STORE_KEY,b.STORE_NAME,b.DISTRICT_NAME,b.OPEN_OR_CLOSED,b.SQUARE_FOOTAGE,b.CHANNEL_NAME,b.FP_OR_SALE_STORE,b.DISTRICT_SORT,b.STORE_SORT,b.FULL_SORT,b.TRAFFIC_COUNTER,b.COMP_DATE,b.IS_COMP,b.OPEN_DATE)")
with fuckit:
    cnx.cursor().execute("drop table tbproduct")
with fuckit:
    cnx.cursor().execute("create or replace table tbproduct as (select * from vwproduct)")
with fuckit:
    cnx.cursor().execute("Merge into tbsalesplan as a using vwsalesplan as b on a.primary_key =b.primary_key when matched then update set DATE_KEY=b.DATE_KEY,STORE_KEY=b.STORE_KEY,SALES_PLAN=b.SALES_PLAN,LY_SALES_PLAN=b.LY_SALES_PLAN,WEEK_SALES_PLAN=b.WEEK_SALES_PLAN,MONTH_SALES_PLAN=b.MONTH_SALES_PLAN,YEAR_MONTH=b.YEAR_MONTH,FISCAL_YEARWEEK=b.FISCAL_YEARWEEK When not matched then insert(PRIMARY_KEY,DATE_KEY,STORE_KEY,SALES_PLAN,LY_SALES_PLAN,WEEK_SALES_PLAN,MONTH_SALES_PLAN,YEAR_MONTH,FISCAL_YEARWEEK) Values (b.PRIMARY_KEY,b.DATE_KEY,b.STORE_KEY,b.SALES_PLAN,b.LY_SALES_PLAN,b.WEEK_SALES_PLAN,b.MONTH_SALES_PLAN,b.YEAR_MONTH,b.FISCAL_YEARWEEK)")
with fuckit:
    cnx.cursor().execute("Merge into tbonhand as a using vwonhand as b on a.primary_key =b.primary_key when matched then update set DATE_KEY=b.DATE_KEY,PRODUCT_KEY=b.PRODUCT_KEY,STORE=b.STORE,INVENTORYUNITS=b.INVENTORYUNITS,INVENTORYVALUE=b.INVENTORYVALUE,INVENTORYESTIMATECOST=b.INVENTORYESTIMATECOST,INVENTORYACTUALCOST=b.INVENTORYACTUALCOST,INVENTORYCORRECTCOST=b.INVENTORYCORRECTCOST When not matched then insert (PRIMARY_KEY,DATE_KEY,PRODUCT_KEY,STORE,INVENTORYUNITS,INVENTORYVALUE,INVENTORYESTIMATECOST,INVENTORYACTUALCOST,INVENTORYCORRECTCOST)Values (b.PRIMARY_KEY,b.DATE_KEY,b.PRODUCT_KEY,b.STORE,b.INVENTORYUNITS,b.INVENTORYVALUE,b.INVENTORYESTIMATECOST,b.INVENTORYACTUALCOST,b.INVENTORYCORRECTCOST)")
with fuckit:
    cnx.cursor().execute("Merge into TBONHAND_FOR_ST_EXPLORE as a using VWONHAND_FOR_ST_EXPLORE as b on a.primary_key =b.primary_key when matched then update set DATE_KEY=b.DATE_KEY,PRODUCT_KEY=b.PRODUCT_KEY,STORE=b.STORE,INVENTORYUNITS=b.INVENTORYUNITS,INVENTORYVALUE=b.INVENTORYVALUE,WEEKINVENTORYUNITS=b.WEEKINVENTORYUNITS,WEEKINVENTORYVALUE=b.WEEKINVENTORYVALUE,INVENTORYESTIMATECOST=b.INVENTORYESTIMATECOST,INVENTORYACTUALCOST=b.INVENTORYACTUALCOST,INVENTORYCORRECTCOST=b.INVENTORYCORRECTCOST When not matched then insert (PRIMARY_KEY,DATE_KEY,PRODUCT_KEY,STORE,INVENTORYUNITS,INVENTORYVALUE,WEEKINVENTORYUNITS,WEEKINVENTORYVALUE,INVENTORYESTIMATECOST,INVENTORYACTUALCOST,INVENTORYCORRECTCOST) values(b.PRIMARY_KEY,b.DATE_KEY,b.PRODUCT_KEY,b.STORE,b.INVENTORYUNITS,b.INVENTORYVALUE,b.WEEKINVENTORYUNITS,b.WEEKINVENTORYVALUE,b.INVENTORYESTIMATECOST,b.INVENTORYACTUALCOST,b.INVENTORYCORRECTCOST)")
with fuckit:
    cnx.cursor().execute("Update tbdate set latest_onhand_date_key = (select max(date_key) from tbonhand)")
with fuckit:
    cnx.cursor().execute("drop table tbdistributions")
with fuckit:
    cnx.cursor().execute("create or replace table tbdistributions as (select * from vwdistributions)")
with fuckit:
    cnx.cursor().execute("Merge into tbsalesplanweekly as a using vwsalesplanweekly as b on a.store_key =b.store_key and a.fiscal_yearweek =b.fiscal_yearweek when matched then update set SALES_PLAN =b.SALES_PLAN When not matched then insert (store_key, fiscal_yearweek, SALES_PLAN) Values (b.store_key, b.fiscal_yearweek, b.SALES_PLAN)")
with fuckit:    
    cnx.cursor().execute("grant select on all tables in schema PRODUCTION.PUBLIC to role looker_role")
with fuckit:
    cnx.cursor().execute("drop table TBSTORETEXTS")
with fuckit:
    cnx.cursor().execute("create or replace table TBSTORETEXTS as (select * from VWSTORETEXTS)")
with fuckit:
    cnx.cursor().execute("drop table TBRCPTADJUSTMENT")
with fuckit:
    cnx.cursor().execute("create or replace table TBRCPTADJUSTMENT as (select * from VWRCPTADJUSTMENT)")
with fuckit:
    cnx.cursor().execute("drop table TBDISTO_RCPT")
with fuckit:
    cnx.cursor().execute("create or replace table TBDISTO_RCPT as (select * from VWDISTO_RCPT)")
with fuckit:
    cnx.cursor().execute("grant select on all tables in schema PRODUCTION.PUBLIC to role looker_role")


