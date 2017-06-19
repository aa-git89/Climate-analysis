# Program to write the ingestion and normalization process for Climate analysis
# This program will iterate through 98,111 files
# Columns which are present in one file but not in the other will contain null values
# This code covers Part 1, 2 and Bonus Q3
library(RMySQL)
library(plyr)
library(stringr)
library(jsonlite)
library(curl)

# set the directory location where files are present
# Edit the path below and set it to the location where all your files are
setwd("C:/Jobs/Tests/gsom_test_data/")

# function to create season, month and year columns
# Takes input as a data frame and outputs the same data frame
# with new columns
prePros <- function(df){
  df$DATE<-as.character(df$DATE)
  df["year"] <- str_sub(df$DATE,1,4)
  df["month"] <- str_sub(df$DATE,-2)
  df["season"] <- ifelse(df$month %in% c("12","01","02"), "Winter",
                        ifelse(df$month %in% c("03","04","05"), "Spring",
                        ifelse(df$month %in% c("06","07","08"), "Summer", 
                        ifelse(df$month %in% c("09","10","11"),"Fall", "Na"))))
  return(df)
}


file_list <- list.files()
# Iterate through the list of files and merge them together
for (file in file_list){
  
  # if the merged dataset doesn't exist, create it
  if (!exists("dataset")){
    dataset <- read.table(file, header=TRUE, sep=",")
    dataset <- prePros(dataset)
}
  
  # if the merged dataset does exist, append to it
  else if (exists("dataset")){
    temp_dataset <- read.table(file, header=TRUE, sep=",")
    temp_dataset <- prePros(temp_dataset)
    dataset<-rbind.fill(dataset, temp_dataset)
    rm(temp_dataset)
  }
  
}

# uncomment the below to see the merged data (Q1)
#dataset 

# Create db connection
# Put the appropriate username, password and dbname for your database connection
mydb = dbConnect(MySQL(), user='root', password='root', dbname='machinelearning'
                 , host='localhost')

#
dbWriteTable(mydb, value = dataset, name = "test_ingest", append = TRUE )

# Question 2
# Below query is used to get the average seasonal temperature 
# per year for all years after 1900 for each 1x1 grid
rs = dbSendQuery(mydb, "select * from (
                select sum(coalesce(TAVG,0))/3 AVG_TEMP, LATITUDE, LONGITUDE, season, year
                from test_ingest
                where year > 1900
                group by  LATITUDE, LONGITUDE, season, year) temp
                where AVG_TEMP > 0;")

# The variable stores the average seasonal temprature in a data frame
data_avg_temp = fetch(rs, n=-1)

# uncomment to see the data for avg. season temp. Data for Q2 is stored in data_avg_temp
#data_avg_temp 


# Bonus 3, Used google API to get the location of the station
lat_lng = dbSendQuery(mydb, "SELECT distinct STATION, LATITUDE, LONGITUDE
                             FROM test_ingest;")
df_service_latlng <- fetch(lat_lng, n=-1)

# Iterate through the distinct stations and get the country name
for(i in 1:nrow(df_service_latlng)){
  lat = df_service_latlng[i,"LATITUDE"]
  lng = df_service_latlng[i,"LONGITUDE"]
  url = paste("https://maps.googleapis.com/maps/api/geocode/json?latlng=",lat,",",lng,sep="")
  result = tryCatch({
              country <- fromJSON(url)}, error = 
                function(e) { 
                  print("Invalid request moving on")
                  return("False")}
  )
  
  if(result == "False")
    next
  else{
    country_subs <- length(country$results$address_components)
    df_service_latlng[i, "country"] <- 
                country$results$address_components[[country_subs]]$long_name
  }
}

# uncomment to see the country. Data for Bonus 3 is in df_service_latlng
# df_service_latlng 
dbDisconnect(mydb)