# NESTE GANG SKAL dataen hentes ut og sendes riktig til SQL.
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
            host="10.0.12.233",
            database="postgres",
            user="postgres",
            password="ntnu",
            port="5432")
        print("Successfully connected to database!")
        
    def set_topics (self, topic1, topic2, topic3, topic4):
        self.topic1 = topic1
        self.topic2 = topic2
        self.topic3 = topic3
        self.topic4 = topic4
    
    def get_client (self):
        return self.mqtt_client
        
    def fromJson (self, jsonString):
        env_dict = json.loads(jsonString)
        self.humidity = env_dict.get("Humidity")
        self.temperature = env_dict.get("Temperature")
        self.co2Equivalent = env_dict.get("Co2 equivalent")
        self.breathVocEquivalent = env_dict.get("Breath voc equivalent")
        self.pressure = env_dict.get("Pressure")
    
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Successfully connected to mqtt-broker!")
            self.mqtt_client.subscribe(self.topic1)
            self.mqtt_client.subscribe(self.topic2)
            self.mqtt_client.subscribe(self.topic3)
            self.mqtt_client.subscribe(self.topic4)
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
        
        self.fromJson (_msg)
        query = None
        timestamp = dt.now()

        # ============== INSERTING SENSOR VALUES ==================
        if msg['topic'] == self.topic1:
            query = f"""INSERT INTO IoT_Assignment_5_Room1 (Time, Humidity, Temperature, co2Equivalent, BreathVocEquivalent, Pressure) VALUES(%s, %s, %s, %s, %s, %s);"""
        elif msg['topic'] == self.topic2:
            query = f"""INSERT INTO IoT_Assignment_5_Room2 (Time, Humidity, Temperature, co2Equivalent, BreathVocEquivalent, Pressure) VALUES(%s, %s, %s, %s, %s, %s);"""
        elif msg['topic'] == self.topic3:
            query = f"""INSERT INTO IoT_Assignment_5_Room3 (Time, Humidity, Temperature, co2Equivalent, BreathVocEquivalent, Pressure) VALUES(%s, %s, %s, %s, %s, %s);"""
        elif msg['topic'] == self.topic4:
            query = f"""INSERT INTO IoT_Assignment_5_Room4 (Time, Humidity, Temperature, co2Equivalent, BreathVocEquivalent, Pressure) VALUES(%s, %s, %s, %s, %s, %s);"""
    
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (timestamp, self.humidity, self.temperature, self.co2Equivalent, self.breathVocEquivalent, self.pressure))
                self.conn.commit()
        except psycopg2.DatabaseError as error:
            print(error)
            return