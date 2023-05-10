#include "bsec.h"
#include "sam.h"
#include <WiFiNINA.h>
#include <ArduinoMqttClient.h>
#include <ArduinoJson.h>
#include <ArduinoLowPower.h>
#include <Adafruit_SleepyDog.h>
#include <RTCZero.h>

char ssid[]="NTNU-IOT"; //network name
char pass[]="password"; //network password

WiFiClient wifiClient;
MqttClient mqttClient(wifiClient);
RTCZero rtc;

const char broker[]="158.38.66.180";
int port = 1883;

const char user[]="ntnu";
const char passMQTT[]="ntnuais2103";
const char topic[]="NTNU/Group23_2/Room2/Sensordata";

const long interval = 1000;
unsigned long previousMillis = 0;
float sleepTimeMin = 1;                 // Minutes
int sleepTimeSec = sleepTimeMin * 60;   // Seconds

// Helper functions declarations
void checkIaqSensorStatus(void);
void errLeds(void);

// Create an object of the class Bsec
Bsec iaqSensor;
String output;

// Create a JSON object to hold the sensor data
StaticJsonDocument<128> jsonDoc;

// Entry point for the example
void setup(void)
{
  /* Initializes the Serial communication */
  Serial.begin(115200);
  delay(1000);
  pinMode(LED_BUILTIN, OUTPUT);
  iaqSensor.begin(BME68X_I2C_ADDR_LOW, Wire);
  output = "\nBSEC library version " + String(iaqSensor.version.major) + "." + String(iaqSensor.version.minor) + "." + String(iaqSensor.version.major_bugfix) + "." + String(iaqSensor.version.minor_bugfix);
  Serial.println(output);
  checkIaqSensorStatus();

  Serial.print("Attempting to connect to WPA SSID: ");
  Serial.println(ssid);
  while (WiFi.begin(ssid) != WL_CONNECTED) {
  // failed, retry
  Serial.print(".");
  delay(5000);
  }

  Serial.println("You're connected to the network");
  Serial.println();

  mqttClient.setUsernamePassword(user, passMQTT);

  while(!mqttClient.connect(broker, port))
  {
    Serial.print("MQTT connection failed. Error: ");
    Serial.println(mqttClient.connectError());
    while(1);
  }

  Serial.println("You're connected to the MQTT broker!");
  Serial.println();


  bsec_virtual_sensor_t sensorList[13] = {
    BSEC_OUTPUT_IAQ,
    BSEC_OUTPUT_STATIC_IAQ,
    BSEC_OUTPUT_CO2_EQUIVALENT,
    BSEC_OUTPUT_BREATH_VOC_EQUIVALENT,
    BSEC_OUTPUT_RAW_TEMPERATURE,
    BSEC_OUTPUT_RAW_PRESSURE,
    BSEC_OUTPUT_RAW_HUMIDITY,
    BSEC_OUTPUT_RAW_GAS,
    BSEC_OUTPUT_STABILIZATION_STATUS,
    BSEC_OUTPUT_RUN_IN_STATUS,
    BSEC_OUTPUT_SENSOR_HEAT_COMPENSATED_TEMPERATURE,
    BSEC_OUTPUT_SENSOR_HEAT_COMPENSATED_HUMIDITY,
    BSEC_OUTPUT_GAS_PERCENTAGE
  };

  iaqSensor.updateSubscription(sensorList, 13, BSEC_SAMPLE_RATE_LP);
  checkIaqSensorStatus();
  // Sleep timer
  rtc.begin();
  rtc.setEpoch(0);

  rtc.enableAlarm (rtc.MATCH_MMSS);
  rtc.attachInterrupt(wakeFromSleep);
}
// Function that is looped forever
void loop(void)
{
  mqttClient.poll();
  checkConnection(wifiClient, mqttClient, ssid, broker, port, user, passMQTT);
  unsigned long currentMillis = millis();

  if (iaqSensor.run())
  { 
    // If new data is available
    jsonDoc["Humidity"] = iaqSensor.humidity;
    jsonDoc["Temperature"] = iaqSensor.temperature;
    jsonDoc["Co2 equivalent"] = iaqSensor.co2Equivalent;
    jsonDoc["Breath voc equivalent"] = iaqSensor.breathVocEquivalent;
    jsonDoc["Pressure"] = iaqSensor.pressure;
    
    // JSON object to string
    String jsonStr;
    serializeJson(jsonDoc, jsonStr);

    mqttClient.beginMessage(topic);
    mqttClient.print(jsonStr);
    mqttClient.endMessage();
    Serial.println(jsonStr);
  }
  else 
  {
    checkIaqSensorStatus();
  }
  delay (200);
  sleep();
}

void sleep ()
{
  WiFi.disconnect();
  WiFi.end();
  rtc.setAlarmEpoch(rtc.getEpoch() + sleepTimeSec);
  rtc.enableAlarm(rtc.MATCH_MMSS);
  rtc.standbyMode();
}
// Tried using low power library but the device never woke up after going to sleep.
// void sleep (void)
// {
//   / Serial.println("Going into deep sleep...");
//   // LowPower.deepSleep(sleepTime); // Putting sensor in deep sleep mode for power saving
//   // Serial.println("Woke up from sleep.");
//   Serial.println("Going to sleep");
//   int sleepMS = Watchdog.sleep();
//   Serial.print("I'm awake now! I slept for ");
//   Serial.print(sleepMS, DEC);
//   Serial.println(" milliseconds.");
//   Serial.println();
// }

// Helper function definitions
void checkIaqSensorStatus(void)
{
  if (iaqSensor.bsecStatus != BSEC_OK) {
    if (iaqSensor.bsecStatus < BSEC_OK) {
      output = "BSEC error code : " + String(iaqSensor.bsecStatus);
      Serial.println(output);
      for (;;)
        errLeds(); /* Halt in case of failure */
    } else {
      output = "BSEC warning code : " + String(iaqSensor.bsecStatus);
      Serial.println(output);
    }
  }

  if (iaqSensor.bme68xStatus != BME68X_OK) {
    if (iaqSensor.bme68xStatus < BME68X_OK) {
      output = "BME68X error code : " + String(iaqSensor.bme68xStatus);
      Serial.println(output);
      for (;;)
        errLeds(); /* Halt in case of failure */
    } else {
      output = "BME68X warning code : " + String(iaqSensor.bme68xStatus);
      Serial.println(output);
    }
  }
}

void errLeds(void)
{
  pinMode(LED_BUILTIN, OUTPUT);
  digitalWrite(LED_BUILTIN, HIGH);
  delay(100);
  digitalWrite(LED_BUILTIN, LOW);
  delay(100);
}

void checkConnection(WiFiClient& wifiClient, MqttClient& mqttClient, const char* ssid, const char* broker, int port, const char* user, const char* passMQTT) {
  // Check WiFi connection
  if (WiFi.status() != WL_CONNECTED) {
    Serial.print("WiFi connection lost, attempting to reconnect...");
    WiFi.disconnect();
    delay(1000);
    WiFi.begin(ssid);
    unsigned long startTime = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - startTime < 10000) {
      delay(500);
      Serial.print(".");
    }
    Serial.println();
    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("Failed to reconnect to WiFi");
    }
    Serial.println("WiFi reconnected");
  }

  // Check MQTT connection
  if (!mqttClient.connected()) {
    Serial.print("MQTT connection lost, attempting to reconnect...");
    while(!mqttClient.connect(broker, port))
    {
      Serial.print("MQTT connection failed. Error: ");
      Serial.println(mqttClient.connectError());
      while(1);
    }
    Serial.println("MQTT reconnected");
  }
}

void wakeFromSleep ()
{
  NVIC_SystemReset();
}