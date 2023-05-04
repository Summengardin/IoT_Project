from src import aranet_handling

# ============ API ===============
# API BASE URL: https://aranet.cloud/api/
# API KEY (NTNU): "drfk8t5k6tb576ndykq65wwyn3tee3b4"
# API KEY (RTU):  "sut582nzqzc8c6utvryxvqecucmbb5dg"
# API NAME: "https://aranet.cloud/openapi/#/"

api_key = "drfk8t5k6tb576ndykq65wwyn3tee3b4" # NTNU
#api_key = "sut582nzqzc8c6utvryxvqecucmbb5dg" # RTU


Aranet_NTNU = aranet_agris.pyAranetDashboard(apikey = api_key_ntnu)
Aranet_RTU = aranet_agris.pyAranetDashboard(apikey = api_key_rtu)

def main():
    ARA = aranet_handling.ARANET(api_key = api_key)
    ARA.list_sensor_names()


if __name__ == '__main__':
    main()
    while True:
        none = None