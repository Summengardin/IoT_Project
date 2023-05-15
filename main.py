from src import aranet_agris
from src import postgres_handling as pg_handle
import pandas as pd
import psycopg2
from datetime import datetime as dt
import numpy as np

# ============ API ===============
# API BASE URL: https://aranet.cloud/api/
# API KEY (NTNU): "drfk8t5k6tb576ndykq65wwyn3tee3b4"
# API KEY (RTU):  "sut582nzqzc8c6utvryxvqecucmbb5dg"
# API NAME: "https://aranet.cloud/openapi/#/"
api_key_ntnu = "drfk8t5k6tb576ndykq65wwyn3tee3b4" # NTNU
api_key_rtu = "sut582nzqzc8c6utvryxvqecucmbb5dg" # RTU


# ====== SELECTED SENSORS FOR THIS ASSIGNMENT ======
selected_sensors_ntnu = {   '4215161' : "Room5",  #NTNU, L163 
                            '4215164' : "Room6",  #NTNU, L165
                            '4215168' : "Room7",  #NTNU, L167
                            '4215190' : "Room8"}  #NTNU, L160

selected_sensors_riga = {   '4209303' : "Room9",  #RIGA, C3323
                            '4209311' : "Room10", #RIGA, C3321
                            '4210130' : "Room11", #RIGA, C1005
                            '4210280' : "Room12"} #RIGA, C1118


# ====== PARAMETERS ======
CYCLE_TIME = 60 # [seconds]. How often the API calls shall run


# ====== DEFINING THE ARANET API-CALLS ======
Aranet_NTNU = aranet_agris.pyAranetDashboard(apikey = api_key_ntnu)
Aranet_RTU = aranet_agris.pyAranetDashboard(apikey = api_key_rtu)


# ====== READ THE LAST MEASURED VALUES ======
def getLastReadings():
    sensor_ids_ntnu = list(map(str, selected_sensors_ntnu.keys()))
    sensor_ids_riga = list(map(str, selected_sensors_riga.keys()))
    
    df_ntnu = Aranet_NTNU.getHistory(sensor_ids=sensor_ids_ntnu)
    df_riga = Aranet_RTU.getHistory(sensor_ids=sensor_ids_riga)

    df = pd.concat([df_ntnu, df_riga])
    df = df.reset_index(drop = True)

    return df

# ====== FILL THE DB WITH DATA FROM THE LAST N DAYS OR MINUTES ======
def fillDb(db = None, trunc = False):
    print("Filling the database with historic data...")

    if db is None: 
        print("Done filling the database")
        return

    sensor_ids_ntnu = list(map(str, selected_sensors_ntnu.keys()))
    sensor_ids_riga = list(map(str, selected_sensors_riga.keys()))
    selected_sensors = selected_sensors_ntnu | selected_sensors_riga

    
    df = pd.DataFrame()
    for sensor_id in sensor_ids_ntnu:
        df = pd.concat([df, Aranet_NTNU.getHistory(sensor_ids=sensor_id, days=2)])

    for sensor_id in sensor_ids_riga:
        df = pd.concat([df, Aranet_RTU.getHistory(sensor_ids=sensor_id, days=2)])
    
    df = df.reset_index(drop = True)
   

    for sensorid in selected_sensors.keys():
        df_sub = df.loc[df['sensorid'] == sensorid]
        df_sub = df_sub.rename(columns={'Atmospheric Pressure':'pressure', 'CO₂':'co2_equivalent', 'Humidity': 'humidity', 'Temperature':'temperature', 'datetime':'time'})

        df_sub = df_sub[['pressure', 'co2_equivalent', 'humidity', 'temperature', 'time']]
        table_name = 'iot_assignment_5_' + selected_sensors[sensorid]
        db.dfToDB(df_sub, table_name, trunc = trunc)

    print("Done filling the database")
    


# ====== ADD NEW DATA IF IT IS NOT ALREADY PRESENT ======
def checkForUpdates(db = None):
    if db is None:
        return
    selected_sensors = selected_sensors_ntnu | selected_sensors_riga
    df = getLastReadings()

    updated = 0
    for i in range(df.shape[0]):
        row = df.iloc[i]
        table_name = 'iot_assignment_5_' + selected_sensors[row['sensorid']]
        row_uptodate = db.valueIsPresent(table_name, 'time', row['datetime'].to_pydatetime().replace(tzinfo=None))
        if not row_uptodate:
            db.storeToDB(series = row, table_names = selected_sensors)
            updated += 1
    print(f"{updated} tables got updated")


def main():

    print(getLastReadings())

    db = pg_handle.Database()

    start_time = dt.now()

    fillDb(db, trunc=True)
    
    while True:
        now = dt.now()
        if (now - start_time).total_seconds() >= CYCLE_TIME:
            start_time = dt.now()
            checkForUpdates(db)

    # Store to databases
    #db.storeToDB(df = df, table_names = (selected_sensors_ntnu | selected_sensors_riga))

    


if __name__ == '__main__':


    main()