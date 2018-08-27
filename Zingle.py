import requests
import json
import keyring
import urllib.parse
import snowflake.connector
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import time
import os


# this creates a log file for the script
import logging
logging.basicConfig(
    filename='C:/python36/scripts/logs/snowflake_python_connector.log',
    level=logging.INFO)

USER = 'PYTHON_SERVICE'
PASSWORD = keyring.get_password("Snowflake", "PYTHON_SERVICE")


ACCOUNT = 'lillypulitzer'
WAREHOUSE = 'STITCH_TEST'
DATABASE = 'staging'
SCHEMA = 'Python_production'

username = 'reportingservices@lillypulitzer.com'
password = keyring.get_password("Zingle", "reportingservices@lillypulitzer.com")

os.environ['TZ']='GMT'
pattern = '%Y-%m-%d %H:%M:%S.%f'
days = 1


# open connection to Snowflake instance
cnx = snowflake.connector.connect(user=USER,
                                  password=PASSWORD,
                                  account=ACCOUNT,
                                  warehouse=WAREHOUSE,
                                  database=DATABASE,
                                  schema=SCHEMA)

#for days in range(dayscount, 0, -1):
yesterday = datetime.now() - timedelta(days=days)
d = yesterday.strftime('%Y-%m-%d') + " 00:00:00.00"

today = datetime.now() - timedelta(days=(days-1))
d2 = today.strftime('%Y-%m-%d') + " 00:00:00.00"


epoch = int(time.mktime(time.strptime(str(d), pattern)))
e = urllib.parse.quote_plus('(' + str(epoch) + ')')

epoch2 = int(time.mktime(time.strptime(str(d2), pattern)))
e2 = urllib.parse.quote_plus('(' + str(epoch2) + ')')

# Get all stores(services) from Zingle
r = requests.get('https://api.zingle.me/v1/services', auth=HTTPBasicAuth(username,password))
response_native = json.loads(r.text)


# For each store parse the 3 digit store number
# Get the total # of contacts each store has
# Get the amount of inbound and outbound text messages for last 24 hours
for si in response_native['result']:

    service_id = si['id']
    store = si['display_name']

# this api call retrieves all messages for the store within the last 24 hours
    r = requests.get('https://api.zingle.me/v1/services/' + service_id +
                     '/messages/?page_size=1000&created_at=greater_than' + e + ',less_than' + e2,
                     auth=HTTPBasicAuth(username,password))

    response_native = json.loads(r.text)

    # delete any existing rows from a previous run
    # this allows the job to be re-run if it fails for any reason

    print("Store:" + store[1:4])
    sql = "DELETE FROM STAGING.ZINGLE.STORE_TEXTS WHERE DATE_TIME = %s"
    args = str(d)
    cnx.cursor().execute(sql, args)

# iterate through the dictionary (response_native) and simply count inbound and outbound msgs
    try:
        if json.loads(r.text)['status']['text'] == 'OK':
            for message in response_native['result']:
                if message['body'].strip() != '':
                    text_id = message['id']
                    store_nbr = store[1:4]
                    date_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(message['created_at']))
                    text_body = message['body']
                    phone_nbr = message['sender']['channel']['value'][2:12]
                    if (message['communication_direction']) == 'inbound':
                        inout_ind = 'in'
                    else:
                        inout_ind = 'out'


                    #create parameterized statement
                    sql = "INSERT INTO STAGING.ZINGLE.STORE_TEXTS VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    args = text_id, store[1:4], service_id, date_time, text_body, phone_nbr, inout_ind

                    cnx.cursor().execute(sql, args)
    except:
        print("Exception: " + text_id)

# finished, close the connection to Snowflake
cnx.close()
