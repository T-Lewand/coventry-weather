
import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium import webdriver
from datetime import datetime
import calendar
import os
import time
import pandas as pd


class WeatherScrapper:
    def __init__(self, year, month):
        """
        timeanddate.com scrapper for temperature historic data in London.
        :param year: Year
        :param month: Month to collect temperature data
        """

        self.year = year
        self.month = month
        self.days = calendar.monthrange(self.year, self.month)[1]
        self.url = f'https://www.timeanddate.com/weather/uk/london/historic?month={month}&year={year}'
        chromedriver = 'C:\\Users\\lewan\\Downloads\\chromedriver'  # This needs to be set individually
        os.environ['webdriver.chrome.driver'] = chromedriver
        self.driver = webdriver.Chrome(chromedriver)

    def _load_html(self, day, latency):
        """
        Loads web page for given year and month
        :return: html object to be read with BeatifulSoup
        """

        self.driver.get(self.url)
        element = self.driver.find_element(by=By.CLASS_NAME, value='weatherLinks')
        href = f'/weather/uk/london/historic?hd={self.year}{self.month:02d}{day:02d}'
        link = self.driver.find_element(By.XPATH, value=f'//a[@href="{href}"]')
        self.driver.execute_script('arguments[0].click()', link)
        time.sleep(latency)
        html = self.driver.page_source
        return html

    def _load_weather_table(self, day, latency):
        """
        Reads weather table with historic data
        :param latency: wait time in seconds
        :return: table rows with temperature
        """

        html = self._load_html(day, latency)
        soup = BeautifulSoup(html, 'html.parser')
        all_tables = soup.find_all('table')
        weather_table = all_tables[1].children
        table_content = []

        for i in weather_table:
            table_content.append(i)

        body = table_content[1]
        rows = body.find_all('tr')

        return rows

    def get_weather(self, latency=0.5):
        """
        Collects temperature data for all days in given month
        :param latency: wait time in seconds
        :return: DataFrame with temperature and timestamp
        """

        data_df = pd.DataFrame()
        for day in range(1, self.days+1):
            while True:
                rows = self._load_weather_table(day, latency)
                data = []
                day_from_html = int(rows[0].find_all('th')[0].text.split()[1])
                date = f'{self.year}-{self.month:02d}-{day_from_html:02d}'
                if day != day_from_html:
                    print('---Missed day, doing again---')
                    print(f'Looking for {day} day, instead got {day_from_html} from html')
                    print('-'*10)
                else:
                    break

            for i in rows:
                time_text = i.find_all('th')[0].text[0:5].split(':')
                time = f'{time_text[0]}:{time_text[1]}'
                timestamp_string = f'{date} {time}'
                timestamp = datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M')

                temp_text = i.find_all('td')[1].text
                temp_value = float(temp_text.split()[0])
                description = i.find_all('td')[2].text[0:-1]
                entry = {'timestamp': timestamp, 'temperature': temp_value, 'description': description}
                data.append(entry)

            data_df = data_df.append(pd.DataFrame(data))
        data_df.reset_index(inplace=True)
        data_df.drop('index', inplace=True, axis=1)

        return data_df

    def get_daylength(self):
        """
        Collects length of day in hours for all days in given month
        :return: list of dict
        """
        url = f'https://www.timeanddate.com/sun/uk/london?month={self.month}&year={self.year}'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        month_daylength = []
        for day in range(1, self.days + 1):
            day_data = soup.find('tr', {'data-day': f'{day}'})
            daylength = day_data.find('td', {'class': 'c tr sep-l'}).text
            daylength = daylength.split(':')
            hour, minutes, seconds = float(daylength[0]), float(daylength[1]), float(daylength[2])
            daylength = hour + minutes/60 + seconds/60/60
            day = {'timestamp': datetime.strptime(f'{self.year}-{self.month}-{day}', '%Y-%m-%d'),
                   'daylength': daylength}
            month_daylength.append(day)

        return month_daylength


class DataGather:
    def __init__(self, start_year: int, start_month: int, end_year: int, end_month: int):
        """
        :param start_year: Year of start of data collection period
        :param start_month: Month of start of data collection period
        :param end_year: Year of end of data collection period
        :param end_month: Month of end of data collection period
        """
        start_time_string = f'{start_year}-{start_month}-01'
        end_time_string = f'{end_year}-{end_month}-01'
        self.start_stamp = datetime.strptime(start_time_string, '%Y-%m-%d')
        self.end_stamp = datetime.strptime(end_time_string, '%Y-%m-%d')
        self.dates = pd.date_range(self.start_stamp, self.end_stamp, freq='MS').to_list()

    def collect_weather(self, latency=0.5, save=True):
        """
        Collects temperature data for given period
        :param latency: wait time in seconds
        :param save: if True saves data to a csv file in working directory
        :return: DataFrame with temperature and timestamps
        """
        data = pd.DataFrame()
        for i in self.dates:
            year = i.year
            month = i.month
            print(f'Collecting for {year}-{month:02d}')
            scrapper = WeatherScrapper(year, month)
            month_data = scrapper.get_weather(latency)
            data = data.append(month_data)
        data.reset_index(inplace=True)
        data.drop('index', inplace=True, axis=1)
        if save:
            data.to_csv('London_weather.txt')

        return data

    def collect_daylight(self, save=True):
        """
        Collects lenght of day for days in given period
        :param save: if True saves data to a csv file in working directory
        :return: DataFrame with length of day and timestamps
        """
        data = pd.DataFrame()
        for i in self.dates:
            year = i.year
            month = i.month
            print(f'Collecting for {year}-{month:02d}')
            scrapper = WeatherScrapper(year, month)
            month_data = scrapper.get_daylength()
            data = data.append(month_data)
        data.reset_index(inplace=True)
        data.drop('index', inplace=True, axis=1)
        if save:
            data.to_csv('London_daylight.txt')
        return data

class WeatherProcessor:
    def __init__(self, filename=None):
        if filename is None:
            self.data = pd.read_csv('London_weather.txt', parse_dates=['timestamp'], index_col=0)

        else:
            self.file = filename
            self.data = pd.read_csv(self.file, parse_dates=['timestamp'], index_col=0)

    def daily(self):
        """
        Creates daily weather data from hourly
        :return: DataFrame
        """
        data = self.data.copy()
        daily = pd.DataFrame()
        for i in range(data.shape[0]):
            data.loc[i, 'timestamp'] = data.loc[i, 'timestamp'].date()

        grouped = data.groupby(by='timestamp')

        for i in grouped:

            index = i[1].index[0]
            day = i[1].loc[index, 'timestamp']
            temp = i[1]['temperature'].mean()
            description = i[1]['description'].mode()

            day_entry = [{'timestamp':day, 'temperature': temp, 'description': description[0]}]
            day_data = pd.DataFrame(day_entry)
            daily = daily.append(day_data)

        daily.reset_index(inplace=True)
        daily.drop('index', inplace=True, axis=1)
        daily['temperature'] = daily['temperature'].round(decimals=1)

        return daily

    def add_daylight(self, daily_data, filename=None):
        if filename is None:
            daylight = pd.read_csv('London_daylight.txt', parse_dates=['timestamp'], index_col=0)
        else:
            daylight = pd.read_csv(filename, parse_dates=['timestamp'], index_col=0)

        data = pd.concat([daily_data, daylight], axis=1)
        data = data.iloc[:, [0, 1, 2, -1]]

        return data

