There are 2 code files
R file covers question 1,2 and bonus question 3
Python file covers bonus question 1

Requirements:
Latest R version and the following libraries

library(RMySQL)
library(plyr)
library(stringr)
library(jsonlite)
library(curl)

Python > 2.7 and python flask, plus the below libraries
import MySQLdb
import json

In the R file:
dataset --> This variable contains the normalized and merged data from all the files
data_avg_temp --> This will store data for question 2
df_service_latlng --> Variable contains the data for bonus question 3

The python program will run and give the result in local host in json format

Assumptions:
1. For normalization, merged data together for all the files and if a particular column is
not available in the file the column remains null for that file
2. TAVG variable is considered the primary variable that has the temperature
3. Bonus Q 1, TAGV, TMIN and TMAX are considered as variables with non-null temperature entries