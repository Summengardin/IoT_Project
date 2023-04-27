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

class MQTT:
    def __init__(self, topic):
        self.topic = topic


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
     7
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
                tempFromDB = pd.DataFrame(cur.fetchall(), columns=['id', 'temp',
    􏰀→ 'timestamp']) # Filter 1
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
     8
    
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
    C Database initializing code
    version: '3.8'
    services:
      postgresdb1:
        container_name: ntnu_db1
        image: postgres
        restart: always
        environment:
          POSTGRES_PASSWORD: Elias123
          POSTGRES_USER: user
          POSTGRES_DB: IOTDB1
        volumes:
          - ./data/dbfiles1:/var/lib/postgresql/data
     9
    
       ports:
        - 5432:5432
    postgresdb2:
      container_name: ntnu_db2
      image: postgres
      restart: always
      environment:
        POSTGRES_PASSWORD: Elias123
        POSTGRES_USER: user
        POSTGRES_DB: IOTDB2
      volumes:
        - ./data/dbfiles2:/var/lib/postgresql/data
      ports:
        - 5433:5432
    pgadmin:
      container_name: pgadmin4
      image: dpage/pgadmin4
      restart: always
      environment:
        PGADMIN_DEFAULT_EMAIL: user@ntnu.no
        PGADMIN_DEFAULT_PASSWORD: ntnu
      ports:
        - "8080:80"
      volumes:
        - ./data/pgadmin:/var/lib/pgadmin