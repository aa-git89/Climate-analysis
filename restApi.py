# This program covers bonus Q 1. It uses Python flask
# For accessing the data use the local host port
# The url will be of the form localhostport/weatherinfo/latitude/longitude
# e.g. http://127.0.0.1:5000/weatherinfo/25.333/55.517
# The last two values are user defined

#!/usr/bin/python
#!flask/bin/python
from flask import Flask
app = Flask(__name__)

hostname = 'localhost'
username = 'root'
password = 'root'
database = 'machinelearning'

# importing required packages
import MySQLdb
import json

# Put the appropriate username and password below to connect to your database
db = MySQLdb.connect( host=hostname, user=username, passwd=password, db=database )
cursor = db.cursor()

@app.route('/weatherinfo/<lati>/<longi>', methods = ['GET'])
# Function that takes latitude and longitude defined by the user. It ouputs
# the avg temp and data points in json format
def coordinate_rest(lati, longi):
    lat = lati
    lng = longi
    # Average seasonal temperature for each season and year where data is available
    sql_avg_temp = """select * from (
                    select sum(coalesce(TAVG,0))/3 AVG_TEMP, season, year
                    from test_ingest
                    where LATITUDE = %s and LONGITUDE = %s
                    group by  season, year) temp
                    where AVG_TEMP > 0"""
    # List of weather stations and number of available datapoints
    # (i.e. non-null temperature entries) for each season and year where data is available
    sql_dta_pts = """select STATION, season, year,count(*) as DATA_PTS
                    from test_ingest
                    where TAVG IS NOT NULL OR TMIN IS NOT NULL OR TMAX IS NOT NULL
                    and LATITUDE = %s and LONGITUDE = %s
                    group by STATION, YEAR, season"""

    cursor.execute(sql_avg_temp, (lat, lng))
    results_avg_temp = [dict((cursor.description[i][0], value) \
               for i, value in enumerate(row)) for row in cursor.fetchall()]
    cursor.execute(sql_dta_pts, (lat, lng))
    results_dta_pts = [dict((cursor.description[i][0], value) \
               for i, value in enumerate(row)) for row in cursor.fetchall()]
    return "The average temp is: \n"+(json.dumps(results_avg_temp)+ "\n"+ "data points are: "+json.dumps(results_dta_pts))+"\n"

if __name__ == '__main__':
     app.run(debug=True)
