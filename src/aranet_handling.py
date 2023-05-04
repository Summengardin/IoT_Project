import requests
import json
import pandas as pd


# OPEN AI KEY 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
 
def run_aranet():
    print('ARANET')


class ARANET:

    REQ_METRICS = 'v1/metrics'
    REQ_SENSORS = 'v1/sensors'
    REQ_BASES = 'v1/bases'
    REQ_HISTORY = 'v1/measurements/history'
    REQ_CURRENT_VALUE = 'v1/measurements/last'
    
    def __init__(self, api = "https://aranet.cloud/api/", api_key = "", interval = 10):
        self.__api_base_url = api
        self.__api_key = api_key
        self.__headers = {
            'accept': 'application/json',
            'ApiKey': f'{self.__api_key}',
        }
        self.interval = interval  # seconds between each call

    def mainloop(self):
        reqs = self.get_reqs()

        sensors_list = self.get_sensors()

        sensors_list = sensors_list[0:10]

        df_select_sensors = self.get_df(reqs.history, sensor_ids=sensors_list, last_n_mins=20, log=False)      
        df_sensordata = self.make_sensor_df_readable(df_select_sensors)
        df_temperatures = df_sensordata.loc[df_sensordata['metric'] == 'Temperature']
        print(df_temperatures)

    def request_json(self, request):

        api_url = f'{self.__api_base_url}{request}'
        response = requests.get(api_url, headers = self.__headers)
        if response.status_code == 200:
            data = json.loads(response.text)
            
        return data

    def get_sensors(self):
        json_data = self.request_json(self.REQ_SENSORS)

        if(json_data is None):
           return
        
        df_prebuild = [[sensor['name'], sensor['type'], sensor['id']] for sensor in json_data['sensors']]
        df = pd.DataFrame(columns=["name", "type", "sensor-id"], data=df_prebuild)
        
        return df
        # Room-name, Type, Sensor-ID

    def get_metrics(self):
        json_data = self.request_json(self.REQ_METRICS)

        if(json_data is None):
           return
        
        print(json.dumps(json.normalize(json_data), indent=2))

        return
    
        df_prebuild = [[sensor['name'], sensor['type'], sensor['sensorId']] for metrics in json_data['sensors']]
        df = pd.DataFrame(columns=["name", "type", "sensor-id"], data=df_prebuild)
        
        return df

    def get_historic_sensor_data(self, sensor_ids = [], days = 1):
        print("***GET SENSOR DATA***")
        sensor_ids_str = ','.join(sensor_ids)
        api_req = f"{self.REQ_HISTORY}?sensor={sensor_ids_str}&days{days}"
        print(api_req)
        json_data = self.request_json(api_req)

        #df_prebuild = [[data['name'], sensor['type'], sensor['id']] for sensor in json_data['sensors']]
        df = pd.json_normalize(json_data)

        return json_data





