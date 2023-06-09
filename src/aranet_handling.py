import pandas as pd
import numpy as np
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import requests
import json
import psycopg2


class pyAranetDashboard(object):
    # --- constructor ---
    def __init__(self, api = "https://aranet.cloud/api/", apikey = "", pg_conn = None):

        # initiate api creds (url_base and key) from provided file
        self.__api_url_base = api
        self.__api_key = apikey

        # initiate base requests from aranet (bases metrics sensors)
        self.__init_requests()

        # populates dict with id's and respective sensor, metric and unit names
        self.__init_names()
    

    #def sensorHasNewValue(self, sensor_id = None):
        


    def getLastReadings(self, sensor_ids=None):

        df_select_sensors = self.get_df(self.__reqs.last, sensor_ids=sensor_ids, log=False)  
        
        df_sensordata = self.make_sensor_df_readable(df_select_sensors)

        
        df_sorted = df_sensordata.sort_values(by='datetime')
        last_values = df_sorted.groupby(['sensorid', 'metric']).agg({'sensor': 'last','value': 'last', 'datetime': 'last'}).reset_index()


        # Pivot the table to create one row for each sensor
        df_pivoted = last_values.pivot_table(index='sensorid', columns=['metric'], values=['sensor', 'value', 'datetime'])

        
        # Find column names
        df_pivoted.columns = [col[-1] for col in df_pivoted.columns.values]

        # Reset the index to make 'sensorid' a column
        df_pivoted = df_pivoted.reset_index()

        datetime_col = df_sorted.groupby('sensorid')['datetime'].first().reset_index()
        df_pivoted = pd.merge(df_pivoted, datetime_col, on='sensorid')

        df_names = pd.DataFrame.from_dict(self.__sensor_names, 'index')
        df_names = df_names.reset_index()
        df_names.columns = ['sensorid','name']

        # Merge sensors_df with original dataframe on 'sensorid' column
        df_with_sensor = pd.merge(df_pivoted, df_names, on = 'sensorid', how='left')

        return df_with_sensor
    

    def getHistory(self, sensor_ids=None, days = None, last_n_mins = None):
        if sensor_ids is None:
            sensor_ids = self.get_sensors()

        if sensor_ids is None:
            return None

        if days is not None or last_n_mins is not None:
            df_select_sensors = self.get_df(self.__reqs.history, sensor_ids=sensor_ids, days=days, last_n_mins=last_n_mins, log=False) 

        # No days or minutes will just return the last value
        else:
            df_select_sensors = self.get_df(self.__reqs.last, sensor_ids=sensor_ids, log=False)
        


        df_sensordata = self.make_sensor_df_readable(df_select_sensors)

        if df_sensordata is None:
            print(f"ERR: Could not recieve data for sensorid: {sensor_ids}")
            return

        # Pivot the data using 'datetime' as the index, 'metric' as the columns, and 'value' as the values
        df_pivoted = df_sensordata.pivot_table(index='datetime', columns='metric', values='value')
        df_pivoted = df_pivoted.reset_index()

        # Merge 'sensorid' and 'sensor' columns from the original DataFrame
        df_pivoted = df_pivoted.merge(df_sensordata[['datetime', 'sensorid', 'sensor']], on='datetime')

        # Reorder the columns in the desired order
        df_pivoted = df_pivoted.drop_duplicates(subset=['datetime', 'sensorid'])

        df_pivoted['datetime'] = [ts.to_pydatetime().replace(tzinfo=None) for ts in df_pivoted['datetime']]

        df_pivoted = df_pivoted.reset_index(drop = True)

        return df_pivoted

    def mainloop(self):
        sensors_list = self.get_sensors()

        #sensors_list = sensors_list[0:10]

        df_select_sensors = self.get_df(self.__reqs.history, sensor_ids=sensors_list, last_n_mins=20, log=False)      
        df_sensordata = self.make_sensor_df_readable(df_select_sensors)

        
        df_sorted = df_sensordata.sort_values(by='datetime')
        last_values = df_sorted.groupby(['sensorid', 'metric']).agg({'sensor': 'last','value': 'last', 'datetime': 'last'}).reset_index()


        # Pivot the table to create one row for each sensor
        pivoted = last_values.pivot_table(index='sensorid', columns=['metric'], values=['sensor', 'value', 'datetime'])

        
        # Find column names
        # pivoted.columns = ['_'.join(col).strip() for col in pivoted.columns.values]
        pivoted.columns = [col[-1] for col in pivoted.columns.values]

        # Reset the index to make 'sensorid' a column
        pivoted = pivoted.reset_index()

        datetime_col = df_sorted.groupby('sensorid')['datetime'].first().reset_index()
        pivoted = pd.merge(pivoted, datetime_col, on='sensorid')

        #pivoted['datetime'] = pivoted.apply(lambda x: x['temperature_datetime'], axis=1)

#
        #df_to_store = pd.DataFrame(columns=['sensor-id', 'timestamp', 'temperature', 'co2', 'humidity', 'pressure'], data = df)

        df_names = pd.DataFrame.from_dict(self.__sensor_names, 'index')
        df_names = df_names.reset_index()
        df_names.columns = ['sensorid','name']

        # Merge sensors_df with original dataframe on 'sensorid' column
        df_with_sensor = pd.merge(pivoted, df_names, on = 'sensorid', how='left')


        # Print the updated dataframe
        print(df_with_sensor)

        #print(pivoted)

    #  --- Sends a request for data and returns the raw json file  ---
    def request_data(self, req, sensor_ids=None, last_n_mins=None, days = None, log=False):

        # Forms url for intended request
        if req is not None and sensor_ids is None and last_n_mins is None and days is None:
            api_url = '{0}{1}'.format(self.__api_url_base, req)
#            print(api_url)

        elif req == self.__reqs.history and sensor_ids is not None and last_n_mins is not None:
            api_url = '{0}{1}?sensor={2}&minutes={3}'.format(
                self.__api_url_base, req, sensor_ids, last_n_mins)
            
        elif req == self.__reqs.history and sensor_ids is not None and days is not None:
            api_url = '{0}{1}?sensor={2}&days={3}'.format(
                self.__api_url_base, req, sensor_ids, days)
            
        elif req == self.__reqs.last and sensor_ids is not None:
            api_url = '{0}{1}?sensor={2}'.format(
                self.__api_url_base, req, sensor_ids)

        else:
            print("ERR in pyAranetDashboard -> get_data():\n  ")
            return None

        # Creates header and sends request
        headers = {
            'accept': 'application/json',
            'ApiKey': '{0}'.format(self.__api_key),
        }
        try:
            response = requests.get(api_url, headers=headers)
            SC = response.status_code
        except Exception as e:
            print("ERR in pyAranetDashboard -> get_data():\n  {0}".format(e))
            return None

        # IF log THEN Outputs the response status code
        if log:
            print("pyAranetDashboard -> \n  Request: GET {0}\n  Response: {1} - {2}".format(
                api_url, SC, self.__status_code_name[SC]))

        # Returns the data or None
        if SC == 200:
            return json.loads(response.text)
        else:
            return None
    
    
    # --- Initializes requests ---
    def __init_requests(self):
        nontupl = ()
        for i in range(len(self.__All_requests.__annotations__)):
            nontupl += (None,)
#        print(nontupl)
        self.__reqs = self.__All_requests(*nontupl)
    
    def get_reqs(self):
        return self.__reqs

    def get_sensors(self):
        df = self.get_df(self.__reqs.sensors)
        return np.unique(df['id'])[1:].tolist()

    # --- Requests all sensors, metrics and units from server and populates dicts ---
    def __init_names(self):
        json_data = self.request_data(self.__reqs.metrics)
        df = pd.json_normalize(json_data["metrics"])
        df = df.reset_index()
        for _, row in df.iterrows():
            self.__metrics_names[row['id']] = row['name']

        df = pd.json_normalize(json_data['metrics'],
                               record_path=['units'])

        df = df.reset_index()
        for _, row in df.iterrows():
            self.__unit_names[row['id']] = row['name']

        json_data = self.request_data(self.__reqs.sensors)
        df = pd.json_normalize(json_data['sensors'])
        df = df.reset_index()
        for _, row in df.iterrows():
            self.__sensor_names[row['id']] = row['name']

    # --- Packs the json file into a Pandas data frame ---   
    def get_df(self, req, sensor_ids=None, last_n_mins=None, days = None, all_sensors=None, log=False):

        # Handles base requests
        if req is not None and sensor_ids is None and last_n_mins is None and days is None:
            json_data = self.request_data(req, log=log)
            #print(json_data)

        # Handles multiple sensor history requests
        elif req is self.__reqs.history and last_n_mins is not None:
            
            if sensor_ids is not None:

                if type(sensor_ids) is list:
                    sensor_ids = '%2C'.join(sensor_ids)

            elif all_sensors:
                sensor_ids = '%2C'.join(self.__sensor_names.keys())

            json_data = self.request_data(
                req, sensor_ids, last_n_mins, log=log)
        # Handles multiple sensor history requests
        elif req is self.__reqs.history and days is not None:

            if sensor_ids is not None:

                if type(sensor_ids) is list:
                    sensor_ids = '%2C'.join(sensor_ids)

            elif all_sensors:
                sensor_ids = '%2C'.join(self.__sensor_names.keys())

            json_data = self.request_data(
                req, sensor_ids, days = days, log=log)
            
        elif req is self.__reqs.last:
            if sensor_ids is not None:

                if type(sensor_ids) is list:
                    sensor_ids = '%2C'.join(sensor_ids)

            elif all_sensors:
                sensor_ids = '%2C'.join(self.__sensor_names.keys())

            json_data = self.request_data(
                req, sensor_ids, log=log)

            # print(json_data)

        # Returns None if arguments don't match
        else:
            print(req)
            print("ERR in pyAranetDashboard -> get_data():\n  ")
            return None
        # Returns None if json is empty
        if json_data == None:
            print("get_df() failed")
            return None

        # Formats the sensors DF
        elif req == self.__reqs.sensors:
#            print(
#                "INF pyAranetDashboard -> get_df():\n  packing 'sensors', dropping 'links'")
            df = pd.json_normalize(json_data['sensors'],
                                   record_path=['skills'],
                                   meta=[
                'id',
                'sensorId',
                'name',
                'type',
                'bases',
            ])

        # Fromtas the metrics DF
        elif req == self.__reqs.metrics:
            df = pd.json_normalize(json_data['metrics'],
                                   record_path=['units'],
                                   meta=[
                'id',
                'name',
            ],
                record_prefix='unit_')

#            print(
#                "INF pyAranetDashboard -> get_df():\n  packing 'metrics', dropping not-default")
            df = df.dropna()

        # Formats the bases DF
        elif req == self.__reqs.bases:
            df = pd.json_normalize(json_data['bases'])

        # Formats the history DF
        elif req == self.__reqs.history:
            try:
                df = pd.json_normalize(json_data['readings'])
            except Exception as e:
                print(
                    "ERR in pyAranetDashboard -> get_df():\n  {0} - try increasing time frame (no measurements received)".format(e))
                if log:
                    print("sensor_ids: {0}".format(sensor_ids))
                return None
            
        # Formats the last DF
        elif req == self.__reqs.last:
            try:
                df = pd.json_normalize(json_data['readings'])
            except Exception as e:
                print(
                    "ERR in pyAranetDashboard -> get_df():\n  {0} - No measurements recieved".format(e))
                if log:
                    print("sensor_ids: {0}".format(sensor_ids))
                return None

        return df
        # --- Adds sensor locations to the DF ---
    def add_locations_to_df(self, df):
        if df is not None:
            df['type'] = df['sensor'].apply(self.__get_stype)
            df['floor'] = df['sensor'].apply(self.__get_sfloor)
            df['room'] = df['sensor'].apply(self.__get_sroom)
            return df
        else:
            print("ERR  add_locations_to_df -> provided df is None!")
            return None

    # --- Returns the available requests for dashboard ---

    def get_reqs(self):
        return self.__reqs

    # --- Prints or fills a list with available sensors ---
    def available_sensors(self, log=False):
        # IF log THEN prints available sensors to command line
        if log:
            print("available sensors:")
            if len(self.__sensor_names) != 0:
                for k, v in self.__sensor_names.items():
                    print("  ", k, v)
            else:
                print("None")

        # If a list is provided, it gets filled with sensor names
        return self.__sensor_names

    # --- Converts metric ids to names ---
    def metric_name(self, id):
        return self.__metrics_names[id]

    # --- Converts sensor ids to names ---
    def sensor_name(self, id):
        return self.__sensor_names[id]

    # --- Converts unit ids to names ---
    def unit_name(self, id):
        return self.__unit_names[id]

    # --- Extracts date from stamp ---
    def extract_date(self, date_time):
        # 2023-01-13T15:27:32Z
        return date_time.strftime("%Y-%m-%d")

    # --- Extracts time from stamp ---
    def extract_time(self, date_time):
        return date_time.strftime("%H:%M:%S")

    def convert_timezone(self, date_time):
        dt = datetime.strptime(date_time, "%Y-%m-%dT%H:%M:%SZ")
        dt = dt.replace(tzinfo=timezone.utc).astimezone(
            tz=timezone(timedelta(hours=2, minutes=0), "Europe/Riga"))
        return dt
    
    # --- Prints or fills a list with available sensors ---
    def available_sensors(self, log=True):
        # IF log THEN prints available sensors to command line
        if log:
            print("available sensors:")
            if len(self.__sensor_names) != 0:
                for k, v in self.__sensor_names.items():
                    print("  ", k, v)
            else:
                print("None")

        # If a list is provided, it gets filled with sensor names
        return self.__sensor_names

    # --- Makes a sensor message readable by replacing ids with names ---
    def make_sensor_df_readable(self, df, include_seperate_date_and_time=True):
        if df is not None:
            df['sensorid'] = df['sensor']
            df['metric'] = df['metric'].apply(self.metric_name)
            df['sensor'] = df['sensor'].apply(self.sensor_name)
            df['unit'] = df['unit'].apply(self.unit_name)
            if include_seperate_date_and_time:
                df['datetime'] = df['time'].apply(self.convert_timezone)
                df['date'] = df['datetime'].apply(self.extract_date)
                df['time'] = df['datetime'].apply(self.extract_time)
            else:
                df['time'] = df['time'].apply(self.convert_timezone)

        else:
            print("ERR  make_sensor_df_readable -> provided df is None!")
        return df

    #  PRIVATE VARIABLES

    __api_url_base = ''
    __api_key = ''
    __status_code_name = {200: "Request OK", 404: "Not Found",
                          500: "Internal server error", 400: "Bad Request"}
    __metrics_names = dict()
    __unit_names = dict()
    __sensor_names = dict()



    @dataclass
    class __All_requests:
          bases: str
          metrics: str
          sensors: str
          history: str
          last: str

          def __post_init__(self):
            if self.bases is None:
                self.bases = 'v1/bases'
            if self.metrics is None:
                self.metrics = 'v1/metrics'
            if self.sensors is None:
                self.sensors = 'v1/sensors'
            if self.history is None:
                self.history = 'v1/measurements/history'
            if self.last is None:
                self.last = 'v1/measurements/last'