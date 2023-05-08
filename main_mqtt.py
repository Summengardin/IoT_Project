import psycopg2

from src import mqtt_handling

# ============ MQTT ==============
# Define mqtt-settings.
MQTT_Broker = "158.38.66.180"
MQTT_Port = 1883
MQTT_Username = 'ntnu'
MQTT_Password = 'ntnuais2103'
MQTT_Keepalive = 5

# Define mqtt-topics
topic1 = 'NTNU/Group23_2/Room1/Sensordata'
topic2 = 'NTNU/Group23_2/Room2/Sensordata'
topic3 = 'NTNU/Group23_2/Room1/Sensordata'
topic4 = 'NTNU/Group23_2/Room1/Sensordata'

if __name__ == '__main__':
    MQTT = mqtt_handling.MQTT_TO_PG(MQTT_Username, MQTT_Password, MQTT_Port, MQTT_Broker,MQTT_Keepalive)
    MQTT.set_topics(topic1, topic2, topic3, topic4)
    MQTT.get_client().loop_forever()
    
# EVT Sende og motta på flere topics