import paho.mqtt.client as mqtt
from datetime import datetime as dt
import pandas as pd
from scipy.signal import butter, filtfilt
import psycopg2
import json

class MQTT_TO_PG:
    def __init__(self, _username, _password, _port, _brokerIp, _keepalive):
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.username_pw_set(username = _username, password = _password)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        print("Connecting to mqtt-broker...")
        self.mqtt_client.connect(_brokerIp, _port, _keepalive)
        print("Connected to mqtt-broker.")
        # ============ POSTGRES ==========
        print("Connecting to database...")
        self.conn = psycopg2.connect(
            host="localhost", # 10.0.12.233
            database="IOTDB1",
            user="user",
            password="abc123",
            port="5432")
        print("Successfully connected to database!")
        
    def set_topics (self, topic1, topic2, topic3, topic4):
        self.topic1 = topic1
        self.topic2 = topic2
        self.topic3 = topic3
        self.topic4 = topic4
    
    def get_client (self):
        return self.mqtt_client
    
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Successfully connected to mqtt-broker!")
            self.mqtt_client.subscribe(self.topic1)
            self.mqtt_client.subscribe(self.topic2)
            self.mqtt_client.subscribe(self.topic3)
            self.mqtt_client.subscribe(self.topic4)
        else:
            print(f"Bad connection. Code: {rc}")
            
    def on_message(self, client, userdata, msg):
        # Decode message payload to string
        payload_str = msg.payload.decode('utf-8')
    
        # Parse message as JSON
        try:
            payload = json.loads(payload_str)
        except ValueError as e:
            print(f'Error parsing payload as JSON: {e}')
            return  
    
        # Determine which table to insert values into based on topic
        if msg.topic == self.topic1:
            table = 'IoT_Assignment_5_Room1'
        elif msg.topic == self.topic2:
            table = 'IoT_Assignment_5_Room2'
        elif msg.topic == self.topic3:
            table = 'IoT_Assignment_5_Room3'
        elif msg.topic == self.topic4:
            table = 'IoT_Assignment_5_Room4'
        else:
            print(f'Unknown topic: {msg.topic}')
            return
    
        # Insert values into database
        timestamp = dt.now()
        query = f'INSERT INTO {table} (time, humidity, temperature, co2_equivalent, breath_voc_equivalent, pressure) VALUES (%s, %s, %s, %s, %s, %s);'
        '''
        values = (timestamp, 
                  payload.get('Humidity'), 
                  payload.get('Temperature'), 
                  payload.get('Co2 equivalent'), 
                  payload.get('Breath voc equivalent'), 
                  payload.get('Pressure'))
        '''
        values = (timestamp, 
                  payload['Humidity'], 
                  payload['Temperature'], 
                  payload['Co2 equivalent'], 
                  payload['Breath voc equivalent'], 
                  payload['Pressure'])
        
        print(values)
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, values)
                self.conn.commit()
        except psycopg2.DatabaseError as e:
            print(f'Error inserting values into {table}: {e}')
        