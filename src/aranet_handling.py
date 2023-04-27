import requests
import json
import pandas as pd





def run_aranet():
    print('ARANET')


class ARANET:
    
    def __init__(self, api = "https://aranet.cloud/api/", api_key = ""):
        self.__api_base_url = api
        self.__api_key = api_key
        self.__headers = {
            'accept': 'application/json',
            'ApiKey': f'{self.__api_key}',
        }


    def connect():
        # Connect
        print('***CONNECT***')


    def request_json(self, request):

        api_url = f'{self.__api_base_url}{request}'
        response = requests.get(api_url, headers = self.__headers)
        if response.status_code == 200:
            data = json.loads(response.text)
            
        return data

    def list_sensor_names(self):
        api_req = 'v1/sensors'
        json_data = self.request_json(api_req)
        if(json_data is None):
           return
        
        df = pd.read_json(json_data)
        print(df)
        # Room-name, Type, Sensor-ID

