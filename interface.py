import pandas as pd
import weather_scrapper as wsc

start_year = 2020
start_month = 2
end_year = 2021
end_month = 3

scrapper = wsc.DataGather(start_year, start_month, end_year, end_month)
data = scrapper.collect()
data.to_csv('weather_data.csv')
