import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import datetime as dt


class Database():

    def __init__(self):
        self.connectToDB()

    def connectToDB(self):
        # ============ POSTGRES ==========
        # print("Connecting to database...")
        # self.conn = psycopg2.connect(
        #     host="localhost", # 10.0.12.233
        #     database="IOTDB1",
        #     user="user",
        #     password="abc123",
        #     port="5432")
        # print("Successfully connected to database!")

        # ============ POSTGRES TEST MARTIN ==========
        print("Connecting to database...")
        self.conn = psycopg2.connect(
            host="localhost", # 10.0.12.233
            database="postgres",
            user="postgres",
            password="master",
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
                        
                        table_name = "IoT_Assignment_5_" + table_name
                        
                        insert_row_sql = f"INSERT INTO {table_name} (pressure, co2_equivalent, humidity, temperature, time) VALUES (%s, %s, %s, %s, %s);"
                        #print(insert_row_sql, (press, co2, humid, temp, timestamp) )
                        cur.execute(insert_row_sql, (press, co2, humid, temp, timestamp))
                self.conn.commit()

        except psycopg2.DatabaseError as e:
            print(f'Error inserting values into {table_name}: {e}')

    def storeToDB(self, series = None, table_names = None):

        if series is None or table_names is None:
            return

        row = series
        # ============== INSERTING SENSOR VALUES ==================
        try:
            with self.conn.cursor() as cur:

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
                    
                    table_name = "IoT_Assignment_5_" + table_name
                    
                    insert_row_sql = f"INSERT INTO {table_name} (pressure, co2_equivalent, humidity, temperature, time) VALUES (%s, %s, %s, %s, %s);"
                    #print(insert_row_sql, (press, co2, humid, temp, timestamp) )
                    cur.execute(insert_row_sql, (press, co2, humid, temp, timestamp))
            self.conn.commit()

        except psycopg2.DatabaseError as e:
            print(f'Error inserting values into {table_name}: {e}')
    

    def valueIsPresent(self, table, field, value):
        query = f"SELECT COUNT(*) FROM {table} WHERE {field} = '{value}'"
    
        with self.conn.cursor() as cur:
            cur.execute(query)
            db_value = cur.fetchone()
        
        return (db_value[0] != 0)


    def dfToDB(self, df = pd.DataFrame(), table_name = None, trunc = False):
        
        if df is None or table_name is None:
            return

        cols = ','.join(list(df.columns))

        data = df.to_numpy()
        query = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols)

        with self.conn.cursor() as cur: 
            if trunc:
                try:
                    cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY")
                    self.conn.commit()
                except  (Exception, psycopg2.DatabaseError) as error:
                    print("Error: %s" % error)
                    print(f"Could not truncate table {table_name}")
            try:
                psycopg2.extras.execute_values(cur, query, data)
                self.conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)
                self.conn.rollback()
        


