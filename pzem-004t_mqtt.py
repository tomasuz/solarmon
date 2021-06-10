#!/usr/bin/python3

'''
Created on 2020-10-03

@author: tomas
'''

import pymodbus
import serial
import math
import paho.mqtt.client as mqtt
import json
import schedule

from pymodbus.pdu import ModbusRequest
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.transaction import ModbusRtuFramer
from time import sleep
from datetime import datetime

def setCounter():
    energy_value_at_time=float(0.0)
    print("Reseting energy_value...")
    
def setCounterAtMidnight():
    energy_value_yesterday = energy_value - energy_value_at_midnight
    energy_value_at_midnight=float(0.0)
    print("Reseting midnight energy_value...")

def calc (registers, factor):
  format = '%%0.%df' % int (math.ceil (math.log10 (factor)))
  if len(registers) == 1:
    return format % ((1.0 * registers[0]) / factor)
  elif len(registers) == 2:
    return format % (((1.0 * registers[1] * 65535) + (1.0 * registers[0])) / factor)
#endif
#end calc

def calc_float (registers, factor):
#  format = '%%0.%df' % int (math.ceil (math.log10 (factor)))
  if len(registers) == 1:
    return (1.0 * registers[0]) / factor
  elif len(registers) == 2:
    return ((1.0 * registers[1] * 65535) + (1.0 * registers[0])) / factor
#endif
#end calc

def read (client):
  result = client.read_input_registers (0x0000, 10, unit = 0x01)
  format = '%%0.%df' % int (math.ceil (math.log10 (1000)))
  if hasattr(result, 'registers') :
    print ('Voltage value: ' + calc (result.registers[0:1], 10) + 'V')
    print ('Current value: ' + calc (result.registers[1:3], 1000) + 'A')
    print ('Power value: ' + calc (result.registers[3:5], 10) + 'W')
    print ('Energy value: ' + (calc (result.registers[5:7], 1000)) + 'kWh')
    print ('Energy value today: ' + format % (calc_float(result.registers[5:7], 1000) - energy_value_at_midnight) + 'kWh')
    print ('Frequency value: ' + calc (result.registers[7:8], 10) + 'Hz')
    print ('Power factor value: ' + calc (result.registers[8:9], 100))
    print ('Alarm status: ' + calc (result.registers[9:10], 1))
  return result 

def energyjson (result):
  energy = {}
  format = '%%0.%df' % int (math.ceil (math.log10 (1000)))
  if hasattr(result, 'registers'):
    energy["TotalStartTime"] = "2019-08-04T16:25:03"
    energy["Total"] = format % (calc_float(result.registers[5:7], 1000) - energy_value_at_time)
    energy["Yesterday"] = energy_value_yesterday
    energy["Period"] = "0"
    energy["Today"] = format % (calc_float(result.registers[5:7], 1000) - energy_value_at_midnight)
    energy["Power"] = calc (result.registers[3:5], 10)
    energy["ApparentPower"] = '0'
    energy["ReactivePower"] = '0'
    energy["Factor"] = calc (result.registers[8:9], 100)
    energy["Voltage"] = calc (result.registers[0:1], 10)
    energy["Current"] = calc (result.registers[1:3], 1000)
    energy["Frequency"] = calc (result.registers[7:8], 10)
    energy["Alarmstatus"] = calc (result.registers[9:10], 1)
  return energy


def on_message(client, userdata, message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)

def on_log(client, userdata, level, buf):
    print("log: ",buf)

broker_address="mqtt.xn--martiiai-9wb.lt"

energy_value=float(0.0)
energy_value_at_time=float(0.0)
energy_value_at_midnight=float(0.0)
energy_value_yesterday=float(0.0)
#broker_address="iot.eclipse.org" #use external broker
mqttclient = mqtt.Client("P1") #create new instance
# mqttclient.on_message=on_message #attach function to callback
# mqttclient.on_log=on_log
mqttclient.connect(broker_address) #connect to broker
# mqttclient.loop_start() #start the loop
# mqttclient.subscribe("house/main-light")
# mqttclient.publish("house/main-light","OFF")#publish
# sleep(4) # wait
# mqttclient.loop_stop() #stop the loop

#energy = {}

#strnow = datetime.now().isoformat(timespec='seconds')
#print(strnow)

#mqttmessage = {}
#mqttmessage["Time"] = strnow
#mqttmessage["ENERGY"] = ""

#json_data = json.dumps(mqttmessage, indent=2)
#print(json_data)

# {"Time":"2020-10-03T20:08:17","ENERGY":{"TotalStartTime":"2019-08-04T16:25:03","Total":2674.596,"Yesterday":1.256,"Today":0.318,"Period":0,"Power":17,"ApparentPower":162,"ReactivePower":161,"Factor":0.10,"Voltage":247,"Current":0.653}}

schedule.every().day.at("08:00").do(setCounter)
schedule.every().day.at("00:00").do(setCounterAtMidnight)

client = ModbusClient (method = "rtu", port="/dev/ttyUSB1", stopbits = 1, bytesize = 8, parity = 'N', baudrate = 9600)

#Connect to the serial modbus server
connection = client.connect()
if client.connect ():
  try:
    while True:
      mqttmessage = {}
      mqttmessage["Time"] = datetime.now().isoformat(timespec='milliseconds')
      readresult = read(client)
      energy_value = calc_float(readresult.registers[5:7], 1000)
      if energy_value_at_time == float(0.0):
        energy_value_at_time = energy_value
      if energy_value_at_midnight == float(0.0):
        energy_value_at_midnight = energy_value

      mqttmessage["ENERGY"] = energyjson(readresult)
      try:
        mqttclient.publish("tele/pzem004t/SENSOR",json.dumps(mqttmessage)) #publish
      except:
        mqttclient.connect(broker_address) # reconect if connection lost.
        mqttclient.publish("tele/pzem004t/SENSOR",json.dumps(mqttmessage)) #publish
      sleep(2)
  finally:
    client.close()
#end try
#end if
