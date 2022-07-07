import sqlite3
import requests
from tqdm import tqdm

import json 
import numpy as np
import pandas as pd
from flask import Flask
app = Flask(__name__) 
    
# Define a function to create connection for reusability purpose
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

# Make a connection
conn = make_connection()

@app.route('/')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

if __name__ == '__main__':
    app.run(debug=True, port=4000)