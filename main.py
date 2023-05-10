from src import aranet_agris
from src import postgres_handling as pg_handle
import pandas as pd

# ============ API ===============
# API BASE URL: https://aranet.cloud/api/
# API KEY (NTNU): "drfk8t5k6tb576ndykq65wwyn3tee3b4"
# API KEY (RTU):  "sut582nzqzc8c6utvryxvqecucmbb5dg"
# API NAME: "https://aranet.cloud/openapi/#/"

api_key_ntnu = "drfk8t5k6tb576ndykq65wwyn3tee3b4" # NTNU
api_key_rtu = "sut582nzqzc8c6utvryxvqecucmbb5dg" # RTU

selected_sensors_ntnu = {   4215161 : "Room5",  #NTNU, L163
                            4215164 : "Room6",  #NTNU, L165
                            4215168 : "Room7",  #NTNU, L167
                            4215190 : "Room8"}  #NTNU, L160

selected_sensors_riga = {   4209303 : "Room9",  #RIGA, C3323
                            4209311 : "Room10", #RIGA, C3321
                            4210130 : "Room11", #RIGA, C1005
                            4210280 : "Room12"} #RIGA, C1118

Aranet_NTNU = aranet_agris.pyAranetDashboard(apikey = api_key_ntnu)
Aranet_RTU = aranet_agris.pyAranetDashboard(apikey = api_key_rtu)

def main():
    # Aranet_NTNU.mainloop()
    # Aranet_RTU.mainloop()
    sensor_ids_ntnu = list(map(str, selected_sensors_ntnu.keys()))
    sensor_ids_riga = list(map(str, selected_sensors_riga.keys()))
    
    df_ntnu = Aranet_NTNU.getLastReadings(sensor_ids=sensor_ids_ntnu)
    df_riga = Aranet_RTU.getLastReadings(sensor_ids=sensor_ids_riga)

    df = pd.concat([df_ntnu, df_riga])

    db = pg_handle.Database()
    db.storeToDB(df = df, table_names = selected_sensors_ntnu | selected_sensors_riga)



if __name__ == '__main__':
    main()
