from flask import Flask, request
import sqlite3
import requests
from tqdm import tqdm
import json 
import numpy as np
import pandas as pd

app = Flask(__name__) 
    
# Define a function to create connection for reusability purpose
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

# Reading the csv data
trips = pd.read_csv('data/austin_bikeshare_trips_2021.csv')
stations = pd.read_csv('data/austin_bikeshare_stations.csv')

# home route
@app.route('/home')
def home():
    return "It's Working!"

# get stations
@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

# add stations
# Get the data values
@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

# get by station_id
@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    stations = get_stations_id(conn,station_id)
    return stations.to_json()

def get_stations_id(conn,station_id):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result


# get trips
@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    stations = get_all_trips(conn)
    return stations.to_json()

def get_all_trips(conn):
    query = f"""SELECT * FROM trips LIMIT 10"""
    result = pd.read_sql_query(query, conn)
    return result

# get by trip_id
@app.route('/trips/<trip_id>')
def route_trip_id(trip_id):
    conn = make_connection()
    stations = get_trip_id(conn,trip_id)
    return stations.to_json()

def get_trip_id(conn,trip_id):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result

# add trips
# Get the data values
@app.route('/trips/add', methods=['POST']) 
def route_add_trips():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

# get duration of bike
@app.route('/trips/average_duration')
def route_average_duration():
    conn = make_connection()
    duration = get_duration(conn)
    return duration.to_json()

def get_duration(conn):
    query = f"""SELECT * FROM trips group by start_station_id"""
    result = pd.read_sql_query(query, conn)
    return result

# get duration of bike by date
@app.route('/trips/average_duration/<bike_id>')
def route_average_duration_bike_id(bike_id):
    conn = make_connection()
    duration = get_duration_bike_id(conn,bike_id)
    return duration.to_json()

def get_duration_bike_id(conn,bike_id):
    query = f"""SELECT * FROM trips WHERE bikeid = {bike_id} group by start_station_id"""
    result = pd.read_sql_query(query, conn)
    return result
    
@app.route('/trips/post', methods=["POST"])
def post_stations():
    conn = make_connection()
    req = request.get_json(force=True)
    period = req['period']
    query = f"SELECT * FROM trips WHERE start_time LIKE '{period}%'"
    selected_data = pd.read_sql_query(query, conn)
    result = selected_data.groupby('start_station_id').agg({
        'bikeid' : 'count', 
        'duration_minutes' : 'mean'
    })
    return result.to_json()

if __name__ == '__main__':
    app.run(debug=True, port=5000)