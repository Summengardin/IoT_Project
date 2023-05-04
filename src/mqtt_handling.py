#from flask import Flask, render_template
#from flask import request
#from flask_mqtt import Mqtt
import paho.mqtt.client as mqtt
from datetime import datetime as dt
import pandas as pd
from scipy.signal import butter, filtfilt
import psycopg2

class MQTT_TO_PG:
    def __init__(self, _username, _password, _port, _brokerIp, _keepalive):
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.username_pw_set(username = _username, password = _password)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        print("Connecting to mqtt-broker...")
        self.mqtt_client.connect(_brokerIp, _port, _keepalive)
        # ============ POSTGRES ==========
        print("Connecting to database...")
        self.conn = psycopg2.connect(
            host="localhost",
            database="db_gui",
            user="postgres",
            password="master",
            port="5432")
        print("Successfully connected to database!")
        
    def set_topics (self, topic1, topic2, topic3):
        self.topic1 = topic1
        self.topic2 = topic2
        self.topic3 = topic3   
        
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Successfully connected to mqtt-broker!")
            self.mqtt_client.subscribe(self.topic1)
            self.mqtt_client.subscribe(self.topic2)
            self.mqtt_client.subscribe(self.topic3)
        else:
            print(f"Bad connection. Code: {rc}")
        
    def on_message(self, client, userdata, _msg):
        # ======= DECODE MESSAGE =======
        msg = dict(
            topic = _msg.topic,
            # payload[0] = time or date, payload[1] = value
            payload=_msg.payload.decode().split(',')
        )
        print(msg)
    
        query = None
    
        # ============== INSERTING SENSOR VALUES ==================
        if msg['topic'] == self.topic1:
            query = f"""INSERT INTO log1 (timestamp, value) VALUES(%s, %s);"""
        elif msg['topic'] == self.topic2:
            query = f"""INSERT INTO log2  (timestamp, value) VALUES(%s, %s);"""
        elif msg['topic'] == self.topic3:
            query = f"""INSERT INTO log3 (timestamp, value) VALUES(%s, %s);"""
    
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, msg['payload'])
                self.conn.commit()
        except psycopg2.DatabaseError as error:
            print(error)
            return



'''
    @app.route('/', methods=['POST', 'GET'])
    @mqtt_client.on_connect()
    def handle_connect(client, userdata, flags, rc):
       if rc == 0:
           print('Connected successfully')
           mqtt_client.subscribe(topic1)
           mqtt_client.subscribe(topic2)
           mqtt_client.subscribe(topic3)
       else:
           print('Bad connection. Code:', rc)
    @mqtt_client.on_message()
    def handle_mqtt_message(client, userdata, message):
        data = dict(
           topic=message.topic,
           payload=message.payload.decode()
        )
        if data['topic'] == topic1:
            request_data = dict(
                topic=topic2,
                payload=message.payload.decode()
            )
            conn = psycopg2.connect(
                host="localhost",
                port="5432",
                database="IOTDB1",
                user="user",
                password="Elias123")
            sql = """INSERT INTO temperature(temp, timestamp) VALUES(%s,%s);"""
            try:
                cur = conn.cursor()
                cur.execute(sql, (data['payload'], dt.now()))
                conn.commit()
                cur.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            sql = """SELECT * FROM temperature ORDER BY timestamp DESC LIMIT 10;"""
            try:
                cur = conn.cursor()
                cur.execute(sql)
                tempFromDB = pd.DataFrame(cur.fetchall(), columns=['id', 'temp','timestamp']) # Filter 1
                order = 2
                f=1
                cut = 0.1
                nyq = 0.5 * f
                cutoff = cut / nyq
                b, a = butter(order, cutoff, btype='low', analog=False)
                filtTemp = filtfilt(b, a, tempFromDB["temp"]) cur.execute(sql, (dt.now(),filtTemp))
                # Filter 2
                temp = 0
                for temperature in filtTemp:
                    temp += temperature
                temp = temp / len(filtTemp)
                publish_result = mqtt_client.publish(topic2, temp)
                print('Mqtt publish code :', publish_result[0])
                cur.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
    if __name__ == '__main__':
        app.run(host='127.0.0.1', port=5000)
    B Web Server 2 code
    from flask import Flask, render_template
    from flask import request
    from flask_mqtt import Mqtt
    import psycopg2
    from datetime import datetime as dt
    import pandas as pd
    from scipy.signal import butter, filtfilt
    app = Flask(__name__)
    app.config['MQTT_BROKER_URL'] = '158.38.67.229'
    app.config['MQTT_BROKER_PORT'] = 1883
    app.config['MQTT_USERNAME'] = 'ntnu'
    app.config['MQTT_PASSWORD'] = 'ntnuais2103'
    app.config['MQTT_KEEPALIVE'] = 5
    app.config['MQTT_TLS_ENABLED'] = False
    topic1 = 'RTUIot/Ard1/BMP280/Temp'
    topic2 ='NTNU/Miniproj/Elias/FiltTemp'
    topic3 ='RTUIot/flask/data'
    
     mqtt_client = Mqtt(app)
    @app.route('/', methods=['POST', 'GET'])
    @mqtt_client.on_connect()
    def handle_connect(client, userdata, flags, rc):
       if rc == 0:
           print('Connected successfully')
           mqtt_client.subscribe(topic1)
           mqtt_client.subscribe(topic2)
           mqtt_client.subscribe(topic3)
       else:
           print('Bad connection. Code:', rc)
    @mqtt_client.on_message()
    def handle_mqtt_message(client, userdata, message):
        data = dict(
           topic=message.topic,
           payload=message.payload.decode()
        )
        if data['topic'] == topic2:
            conn = psycopg2.connect(
                host="localhost",
                port="5433",
                database="IOTDB2",
                user="user",
                password="Elias123")
    sql = """INSERT INTO filttemperature(timestamp, filttemp) 􏰀→ VALUES(%s,%s);"""
            try:
                cur = conn.cursor()
                cur.execute(sql, (dt.now(),data['payload']))
                conn.commit()
                cur.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            finally:
                if conn is not None:
                    conn.close()
    if __name__ == '__main__':
        app.run(host='127.0.0.1', port=5001)
'''
        



'''
import paho.mqtt.client as mqtt
import psycopg2

# Define database-connection (postgresql)
print("Connecting to database...")
conn = psycopg2.connect(
    host="localhost",
    database="db_gui",
    user="postgres",
    password="master",
    port="5434")
print("Successfully connected to database!")

# Define mqtt-settings.
MQTT_BROKER = "158.38.67.229"
MQTT_PORT = 1883
MQTT_USERNAME = 'ntnu'
MQTT_PASSWORD = 'ntnuais2103'
MQTT_KEEPALIVE = 5

# Define mqtt-topics
# subscribe
topic_temperature_filtered = 'RTUIot/marsi/Temp/filtered'
topic_pressure_filtered = 'RTUIot/marsi/Press/filtered'

topic_temperature_aggr = 'RTUIot/marsi/Temp/aggr'
topic_pressure_aggr = 'RTUIot/marsi/Press/aggr'

# Define mqtt-client
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)


# Define "on_connect"-function
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Successfully connected to mqtt-broker!")
        mqtt_client.subscribe(topic_temperature_filtered)
        mqtt_client.subscribe(topic_pressure_filtered)

        mqtt_client.subscribe(topic_temperature_aggr)
        mqtt_client.subscribe(topic_pressure_aggr)
    else:
        print(f"Bad connection. Code: {rc}")


# Define "on_message"-function
def on_message(client, userdata, _msg):
    # ======= DECODE MESSAGE =======
    msg = dict(
        topic=_msg.topic,
        # payload[0] = time or date, payload[1] = value
        payload=_msg.payload.decode().split(',')
    )
    print(msg)

    query = None

    # ============== TEMPERATURE ==================
    if msg['topic'] == topic_temperature_filtered:
        query = f"""INSERT INTO temperature_log (timestamp, value) VALUES(%s, %s);"""
    elif msg['topic'] == topic_pressure_filtered:
        query = f"""INSERT INTO pressure_log (timestamp, value) VALUES(%s, %s);"""
    elif msg['topic'] == topic_temperature_aggr:
        query = f"""MERGE INTO temperature_aggr AS T
	                USING (SELECT '{msg['payload'][0]}'::date AS date, {msg['payload'][1]} AS value_max, {msg['payload'][2]} AS 
                            value_min, {msg['payload'][3]} AS value_avg 
                            ) AS S 
                    ON (T.date = S.date)
                    WHEN MATCHED THEN 
                        UPDATE SET 
                            value_max = S.value_max,
                            value_min = S.value_min, 
                            value_avg = S.value_avg  
                    WHEN NOT MATCHED THEN 
                        INSERT (date, value_min, 
                            value_max, value_avg) VALUES (S.date, S.value_min, S.value_max, S.value_avg);"""
    elif msg['topic'] == topic_pressure_aggr:
        query = f"""MERGE INTO pressure_aggr AS T
        	                USING (SELECT '{msg['payload'][0]}'::date AS date, {msg['payload'][1]} AS value_max, {msg['payload'][2]} AS 
                                    value_min, {msg['payload'][3]} AS value_avg 
                                    ) AS S 
                            ON (T.date = S.date)
                            WHEN MATCHED THEN 
                                UPDATE SET 
                                    value_max = S.value_max,
                                    value_min = S.value_min, 
                                    value_avg = S.value_avg  
                            WHEN NOT MATCHED THEN 
                                INSERT (date, value_min, 
                                    value_max, value_avg) VALUES (S.date, S.value_min, S.value_max, S.value_avg);"""

    try:
        with conn.cursor() as cur:
            cur.execute(query, msg['payload'])
            conn.commit()
    except psycopg2.DatabaseError as error:
        print(error)
        return

# Connect to mqtt-broker
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
print("Connecting to mqtt-broker...")
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)

if __name__ == '__main__':
    mqtt_client.loop_forever()
'''