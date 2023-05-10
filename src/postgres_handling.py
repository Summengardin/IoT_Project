import psycopg2
from datetime import datetime as dt


class Database():

    def __init__(self):
#        self.connectToDB()
        print('Connected to DB')

    def connectToDB(self):
        # ============ POSTGRES ==========
        print("Connecting to database...")
        self.conn = psycopg2.connect(
            host="localhost", # 10.0.12.233
            database="IOTDB1",
            user="user",
            password="abc123",
            port="5432")
        print("Successfully connected to database!")
            
    def storeToDB(self, df = None, table_names = None):

        if df is None or table_names is None:
            return

        data = df.to_dict(orient='records')
        # ============== INSERTING SENSOR VALUES ==================
        try:
            with self.conn.cursor() as cur:
                for row in data:
                    # Get the table name for the current row based on sensorid
                    sensor_id = row['sensorid']

                    table_name = table_names[sensor_id]
                    
                    if table_name is not None:
                        # Insert the current row into the corresponding table
                        press = row['Atmospheric Pressure']
                        co2 = row['CO₂']
                        humid = row['Humidity']
                        temp = row['Temperature']
                        timestamp = row['datetime'].to_pydatetime().replace(tzinfo=None)

                        insert_row_sql = f"INSERT INTO {table_name} (pressure, co2_equivalent, humidity, temperature, time) VALUES (%s, %s, %s, %s, %s);"
                        #print(insert_row_sql, (press, co2, humid, temp, timestamp) )
                        cur.execute(insert_row_sql, (press, co2, humid, temp, timestamp))
    
            cur.commit()

        except psycopg2.DatabaseError as e:
            print(f'Error inserting values into {table_name}: {e}')
