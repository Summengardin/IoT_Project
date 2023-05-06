import psycopg2

from src import mqtt_handling

# ============ API ===============
# API BASE URL: https://aranet.cloud/api/
# API KEY (NTNU): "drfk8t5k6tb576ndykq65wwyn3tee3b4"
# API KEY (RTU):  "sut582nzqzc8c6utvryxvqecucmbb5dg"
# API NAME: "https://aranet.cloud/openapi/#/"

api_key = "drfk8t5k6tb576ndykq65wwyn3tee3b4" # NTNU
#api_key = "sut582nzqzc8c6utvryxvqecucmbb5dg" # RTU

# ============ MQTT ==============
# Define mqtt-settings.
MQTT_Broker = "158.38.66.180"
MQTT_Port = 9001
MQTT_Username = 'ntnu'
MQTT_Password = 'ntnuais2103'
MQTT_Keepalive = 5

# Define mqtt-topics
topic1 = 'Gruppe_23_2/Elias/sensordata'
topic2 = 'Gruppe_23_2/Mathieu/sensordata'
topic3 = 'Gruppe_23_2/Melina/sensordata'
topic4 = 'Gruppe_23_2/Martin/sensordata'

if __name__ == '__main__':
    MQTT = mqtt_handling.MQTT_TO_PG(MQTT_Username, MQTT_Password, MQTT_Port, MQTT_Broker,MQTT_Keepalive)
    MQTT.set_topics(topic1, topic2, topic3, topic4)
    MQTT.get_client().loop_forever()