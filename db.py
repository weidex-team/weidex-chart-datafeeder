from flaskext.mysql import MySQL
import mysql.connector
from flask import Flask
import os

app = Flask(__name__)

retry_count = 5
retry_delay = 12  #in seconds

config = {
  'user': os.environ['MYSQL_DATABASE_USER'],
  'password': os.environ['MYSQL_DATABASE_PASSWORD'],
  'host': os.environ['MYSQL_DATABASE_HOST'],
  'database': os.environ['MYSQL_DATABASE_DB'],
  'port': '3306'
}

conn = mysql.connector.connect(**config)

def reconnect():
    print('INFO:: Trying to reconnect.')
    conn.reconnect(retry_count, retry_delay)
