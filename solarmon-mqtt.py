#!/usr/bin/env python3

import time
from datetime import datetime
import os
import json
from decimal import *
getcontext().prec = 2

from configparser import RawConfigParser
from influxdb import InfluxDBClient
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
import paho.mqtt.client as mqtt

from growatt import Growatt

settings = RawConfigParser()
settings.read(os.path.dirname(os.path.realpath(__file__)) + '/solarmon.cfg')

interval = settings.getint('query', 'interval', fallback=1)
offline_interval = settings.getint('query', 'offline_interval', fallback=60)
error_interval = settings.getint('query', 'error_interval', fallback=60)

db_name = settings.get('influx', 'db_name', fallback='inverter')
measurement = settings.get('influx', 'measurement', fallback='inverter')

broker_address = settings.get('mqtt', 'broker_address', fallback='iot.eclipse.org')
mqtt_subscribe_pzem = settings.get('mqtt', 'subscribe-pzem', fallback='tele/pzem004t/SENSOR')
mqtt_subscribe_growatt = settings.get('mqtt', 'subscribe-growatt', fallback='tele/growatt/SENSOR')
mqtt_measurement = settings.get('mqtt', 'measurement', fallback='grid')

vdiffarr = [0.0] * 5

lastgrowattpower = Decimal('0.0')
lastgridpower = Decimal('0.0')
powerdirection = 1 # 1 - consume power from grid, -1 - supply power to grid.

# Clients
print('Setup InfluxDB Client... ', end='')
influx = InfluxDBClient(host=settings.get('influx', 'host', fallback='localhost'),
                        port=settings.getint('influx', 'port', fallback=8086),
                        username=settings.get('influx', 'username', fallback=None),
                        password=settings.get('influx', 'password', fallback=None),
                        database=db_name)
influx.create_database(db_name)
print('Done!')

print('Setup Serial Connection... ', end='')
port = settings.get('solarmon', 'port', fallback='/dev/ttyUSB0')
client = ModbusClient(method='rtu', port=port, baudrate=9600, stopbits=1, parity='N', bytesize=8, timeout=1)
client.connect()
print('Dome!')

print('Loading inverters... ')
inverters = []
for section in settings.sections():
    if not section.startswith('inverters.'):
        continue

    name = section[10:]
    unit = int(settings.get(section, 'unit'))
    measurement = settings.get(section, 'measurement')
    growatt = None
    try:
        growatt = Growatt(client, name, unit)
    except Exception as err:
        print(err)

    inverters.append({
        'error_sleep': 0,
        'name': name,
        'unit': unit,
        'growatt': growatt,
        'measurement': measurement
    })
print('Done!')

def process_inverters(now):
    info = None
    for inverter in inverters:
        # If this inverter errored then we wait a bit before trying again
        if inverter['error_sleep'] > 0:
            inverter['error_sleep'] -= interval
            continue

        growatt = inverter['growatt']
        try:
            if growatt is None:
                growatt = Growatt(client, inverter['name'], inverter['unit']) 
            print(datetime.now().isoformat(timespec='milliseconds'))
            info = growatt.read()

            if info is None:
                continue
           
            points = [{
                'time': int(now),
                'measurement': inverter['measurement'],
                "fields": info
            }]

            print(growatt.name)
            print(info)

            if not influx.write_points(points, time_precision='s'):
                print("Failed to write to DB!")
        except Exception as err:
            print(inverter['name'])
            print(err)
            inverter['error_sleep'] = error_interval
    return info

def on_message(client, userdata, message):
    global lastgrowattpower
    global lastgridpower
    global powerdirection
    global vdiffarr
    
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

    gridvoltage = Decimal(energy_parsed['Voltage'])
    growattvoltage = gridvoltage
    if growattinfo is None:        
        lastgrowattpower = Decimal('0.0')
        powerdirection = 1
    else:
        growattvoltage = Decimal(growattinfo['Vac1'])       
        
        vdiffarr = vdiffarr[1:]+vdiffarr[:1]
        print("rotated", vdiffarr)
        vdiffarr[len(vdiffarr)-1] = float(growattvoltage - gridvoltage)
        print("updated", vdiffarr)
        
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
            mqttclient.publish(mqtt_subscribe_growatt,json.dumps(mqttmessage)) #publish
        except:
            print("Failed to publish mqtt message to broker!")
            mqttclient.connect(broker_address) # reconect if connection lost.
            mqttclient.publish(mqtt_subscribe_growatt,json.dumps(mqttmessage)) #publish
    energy_parsed['powerdirection'] = powerdirection
    energy_parsed['voltagediff'] = float(growattvoltage - gridvoltage)
    energy_parsed['voltagediffmean'] = sum(vdiffarr) / len(vdiffarr)
    energy_parsed['gridpowerdiff'] = float(gridpowerdiff)
    energy_parsed['growattpowerdiff'] = float(growattpowerdiff)
        
    points = [{
        'time': int(now),
        'measurement': mqtt_measurement,
        "fields": energy_parsed
    }]
    if not influx.write_points(points, time_precision='s'):
        print("Failed to write to DB!")

def on_log(client, userdata, level, buf):
    print("log: ",buf)


print('Setup mqtt Connection... ', end='')
mqttclient = mqtt.Client("SOLARMON")
mqttclient.on_message=on_message #attach function to callback
# mqttclient.on_log=on_log
mqttclient.connect(broker_address) #connect to broker
mqttclient.loop_start() #start the loop
mqttclient.subscribe(mqtt_subscribe_pzem)
print('Done with MQTT!')
# mqttclient.loop_forever()

while True:
    time.sleep(offline_interval)


