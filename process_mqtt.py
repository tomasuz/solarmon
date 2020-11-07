#!/usr/bin/env python3

import os
import time
import json
from decimal import *
getcontext().prec = 2

from configparser import RawConfigParser
import paho.mqtt.client as mqtt

settings = RawConfigParser()
settings.read(os.path.dirname(os.path.realpath(__file__)) + '/solarmon.cfg')

offline_interval = settings.getint('query', 'offline_interval', fallback=60)

broker_address = settings.get('mqtt', 'broker_address', fallback='iot.eclipse.org')
mqtt_subscribe_pzem = settings.get('mqtt', 'subscribe-pzem', fallback='tele/pzem004t/SENSOR')
mqtt_subscribe_growatt = settings.get('mqtt', 'subscribe-growatt', fallback='tele/growatt/SENSOR')
mqtt_measurement = settings.get('mqtt', 'measurement', fallback='grid')

energy_parsed = None

vdiffarr = [0.0] * 5

lastgrowattpower = Decimal('0.0')
lastgridpower = Decimal('0.0')
powerdirection = 1 # 1 - consume power from grid, -1 - supply power to grid.

def on_pzem_message(client, userdata, message):

    getcontext().prec = 2
    global energy_parsed
    payload = str(message.payload.decode("utf-8"))
    print("message received ", payload)
    mqtt_message = json.loads(payload)
    energy = mqtt_message["ENERGY"]
    energy_parsed = {}
    for i, (k, v) in enumerate(energy.items()):
        try:
            energy_parsed[k] = int(v)
        except ValueError:
            try:
                energy_parsed[k] = float(v)
            except ValueError:
                payload = payload


def on_growatt_message(client, userdata, message):
    return
    global energy_parsed
    getcontext().prec = 2

    now = time.time()
    growattinfo = process_inverters(now)

    payload = str(message.payload.decode("utf-8"))
    print("message received ", payload)
    mqtt_message = json.loads(payload)
    energy = mqtt_message["ENERGY"]
    energy_parsed = {}
    for i, (k, v) in enumerate(energy.items()):
        try:
            energy_parsed[k] = int(v)
        except ValueError:
            try:
                energy_parsed[k] = float(v)
            except ValueError:
                payload = payload

    if growattinfo is None:
        lastgrowattpower = Decimal('0.0')
        powerdirection = 1
    else:
        growattpower = Decimal(growattinfo['Pac'])
        gridpower = Decimal(energy_parsed['Power'])
        growattpowerdiff = growattpower - lastgrowattpower
        gridpowerdiff = gridpower - lastgridpower
        print('growattpowerdiff ', growattpowerdiff, 'gridpowerdiff ', gridpowerdiff)
        # If increased growatt power generation increses grid power - we are supplying power to grid:
        if (growattpower < gridpower):
            powerdirection = 1
        elif (growattpowerdiff > 0 and gridpowerdiff > 0):
            powerdirection = -1
        # If decresed growwat power generation increases grid power - we are also supplying power to grid:
        elif (growattpowerdiff < 0 and gridpowerdiff < 0):
            powerdirection = -1
        # if power value does not changed - leave as is
        elif (growattpowerdiff == 0 or gridpowerdiff == 0):
            powerdirection = powerdirection
        else:
            powerdirection = 1
        
        lastgrowattpower = Decimal(growattinfo['Pac'])
        lastgridpower = Decimal(energy_parsed['Power'])
        # publish growatt info to mqtt broker also. 
        mqttmessage = {}
        mqttmessage["Time"] = datetime.now().isoformat(timespec='milliseconds')
        mqttmessage["ENERGY"] = growattinfo
        try:
            mqttclient.publish("tele/growatt/SENSOR",json.dumps(mqttmessage)) #publish
        except:
            mqttclient.connect(broker_address) # reconect if connection lost.
            mqttclient.publish("tele/growatt/SENSOR",json.dumps(mqttmessage)) #publish
    energy_parsed['powerdirection'] = powerdirection
        
    points = [{
        'time': int(now),
        'measurement': mqtt_measurement,
        "fields": energy_parsed
    }]
    if not influx.write_points(points, time_precision='s'):
        print("Failed to write to DB!")
    return

### topic message
def on_message(mosq, obj, msg):
    print(msg.topic+" "+str(msg.qos)+" "+str(msg.payload))

def on_log(client, userdata, level, buf):
    print("log: ",buf)


print('Setup mqtt Connection... ', end='')
mqttclient = mqtt.Client("PROCESS")
mqttclient.message_callback_add(mqtt_subscribe_pzem, on_pzem_message)
mqttclient.message_callback_add(mqtt_subscribe_growatt, on_growatt_message)
mqttclient.on_message=on_message #attach function to callback
# mqttclient.on_log=on_log
mqttclient.connect(broker_address) #connect to broker
mqttclient.loop_start() #start the loop
mqttclient.subscribe([(mqtt_subscribe_pzem, 0), (mqtt_subscribe_growatt, 0)])
print('Done with MQTT!')

while True:
    time.sleep(offline_interval)

