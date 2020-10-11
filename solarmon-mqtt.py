#!/usr/bin/env python3

import time
import os
import json

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
mqtt_subscribe = settings.get('mqtt', 'subscribe', fallback='iot.eclipse.org')
mqtt_measurement = settings.get('mqtt', 'measurement', fallback='grid')

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
    growatt = Growatt(client, name, unit)
    growatt.print_info()
    inverters.append({
        'error_sleep': 0,
        'growatt': growatt,
        'measurement': measurement
    })
print('Done!')

def process_inverters():
    for inverter in inverters:
        # If this inverter errored then we wait a bit before trying again
        if inverter['error_sleep'] > 0:
            inverter['error_sleep'] -= interval
            continue

        growatt = inverter['growatt']
        try:
            now = time.time()
            info = growatt.read()

            if info is None:
                continue

            points = [{
                'time': int(now),
                'measurement': inverter['measurement'],
                "fields": info
            }]

            print(growatt.name)
            print(points)

            if not influx.write_points(points, time_precision='s'):
                print("Failed to write to DB!")
        except Exception as err:
            print(growatt.name)
            print(err)
            inverter['error_sleep'] = error_interval

def on_message(client, userdata, message):
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
        
    now = time.time()
    points = [{
        'time': int(now),
        'measurement': mqtt_measurement,
        "fields": energy_parsed
    }]
    if not influx.write_points(points, time_precision='s'):
        print("Failed to write to DB!")

    process_inverters()

def on_log(client, userdata, level, buf):
    print("log: ",buf)


print('Setup mqtt Connection... ', end='')
mqttclient = mqtt.Client("SOLARMON")
mqttclient.on_message=on_message #attach function to callback
mqttclient.on_log=on_log
mqttclient.connect(broker_address) #connect to broker
mqttclient.loop_start() #start the loop
mqttclient.subscribe(mqtt_subscribe)
print('Done with MQTT!')
