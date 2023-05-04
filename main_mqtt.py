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
MQTT_Port = 1883
MQTT_Username = 'ntnu'
MQTT_Password = 'ntnuais2103'
MQTT_Keepalive = 5

# Define mqtt-topics
topic1 = 'NTNU/IoT/Group13/L160/UnfilteredTemp'
topic2 = 'NTNU/IoT/Group13/L160/UnfilteredPressure'
topic3 = 'NTNU/IoT/Group13/L160/UnfilteredCo2'

if __name__ == '__main__':
    MQTT = mqtt_handling.MQTT_TO_PG(MQTT_Username, MQTT_Password, MQTT_Port, MQTT_Broker,MQTT_Keepalive)
    MQTT.get_client().loop_forever()