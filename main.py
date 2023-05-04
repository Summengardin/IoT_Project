from src import aranet_agris

# ============ API ===============
# API BASE URL: https://aranet.cloud/api/
# API KEY (NTNU): "drfk8t5k6tb576ndykq65wwyn3tee3b4"
# API KEY (RTU):  "sut582nzqzc8c6utvryxvqecucmbb5dg"
# API NAME: "https://aranet.cloud/openapi/#/"

api_key_ntnu = "drfk8t5k6tb576ndykq65wwyn3tee3b4" # NTNU
api_key_rtu = "sut582nzqzc8c6utvryxvqecucmbb5dg" # RTU


Aranet_NTNU = aranet_agris.pyAranetDashboard(apikey = api_key_ntnu)
Aranet_RTU = aranet_agris.pyAranetDashboard(apikey = api_key_rtu)

def main():
    Aranet_NTNU.mainloop()
    Aranet_RTU.mainloop()


if __name__ == '__main__':
    main()
