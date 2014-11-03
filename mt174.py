#!/usr/bin/python

import subprocess
import random
import mosquitto
import serial
import time
import re
import logging


class FakeMeter:

	def __init__(self):
		self.__index = [0, 0]
		logging.info("Created FakeMeter")

	def read(self):
		self.__index = [x + random.random() for x in self.__index]
		total = self.__index[0] + self.__index[1]
		return '''0-0:F.F.0*255(0000000)
1-0:0.0.0*255(352143)
0-0:C.1.0*255(62791737)
C.1.1(ISK0MT174-0001)
1-0:1.8.1*255(%011.3f*kWh)
1-0:1.8.2*255(%011.3f*kWh)
1-0:1.8.0*255(%011.3f*kWh)
1-0:2.8.1*255(0000001.012*kWh)
1-0:2.8.2*255(0000001.612*kWh)
1-0:2.8.0*255(0000002.624*kWh)
1-0:32.7.0*255(233.1*V)
1-0:52.7.0*255(232.7*V)
1-0:72.7.0*255(233.8*V)
1-0:31.7.0*255(5.36*A)
1-0:51.7.0*255(8.06*A)
1-0:71.7.0*255(5.71*A)
1-0:36.7.0*255(1.086*kW)
1-0:56.7.0*255(1.714*kW)
1-0:76.7.0*255(1.220*kW)
1-0:33.7.0*255(0.866)
1-0:53.7.0*255(0.915)
1-0:73.7.0*255(0.911)
0-0:C.7.0*255(5)
0-0:C.7.1*255(5)
0-0:C.7.2*255(5)
0-0:C.7.3*255(5)
1-0:0.2.0*255(1.03)
0-0:C.1.6*255(FDF5)''' % (self.__index[0], self.__index[1], total)



class MT174:

	ACK = '\x06'
	STX = '\x02'
	ETX = '\x03'
	DELAY = 0.02

	def __init__(self, port):
		self.__port = port
		logging.info("Created MT174, port = %s", port)
	
	@staticmethod
	def __delay():
		time.sleep(MT174.DELAY)
	
	def read(self):
		logging.debug("Opening serial port %s", self.__port)
		mt174 = serial.Serial(port = self.__port, baudrate=300, bytesize=7, parity='E', stopbits=1, timeout=1.5);
		try:
			# 1 ->
			logging.debug("Writing hello message")
			message = '/?!\r\n' # IEC 62056-21:2002(E) 6.3.1
			mt174.write(message)
			# 2 <-
			MT174.__delay()
			message = mt174.readline() # IEC 62056-21:2002(E) 6.3.2
			if len(message) == 0:
				raise Exception("Empty string instead of identification")
			logging.debug("Got reply: %s", message)
			if message[0] != '/':
				raise Exception("No identification message")
			if len(message) < 7:
				raise Exception("Identification message to short")
			# 3 ->
			message = MT174.ACK + '000\r\n' # IEC 62056-21:2002(E) 6.3.3
			mt174.write(message)
			MT174.__delay()
			# 4 <-
			datablock = ""
			if mt174.read() == MT174.STX:
				x = mt174.read()
				if len(x) == 0:
					raise Exception("Empty string instead of data")
				BCC = 0
				while x != '!':
					BCC = BCC ^ ord(x)
					datablock = datablock + x
					x = mt174.read()
					if len(x) == 0:
						raise Exception("Empty string instead of data")
				while x != MT174.ETX:
					BCC = BCC ^ ord(x) # ETX itself is part of block check
					x = mt174.read()
					if len(x) == 0:
						raise Exception("Empty string instead of data")
				BCC = BCC ^ ord(x)
				x = mt174.read() # x is now the Block Check Character
				# last character is read, could close connection here
				if (BCC != ord(x)): # received correctly?
					datablock = ""
					raise Exception("Result not OK, try again")
			else:
				logging.warning("No STX found, not handled")
			return datablock
		finally:
			if mt174.isOpen():
				mt174.close()

class Scheduler:

	SLEEP_TIME = 0.1

	def __init__(self, mt174, processors, interval = 60):
		self.__mt174 = mt174
		self.__processors = processors
		self.__interval = interval
		logging.info("Created scheduler, interval = %ds", interval)
	
	def execute(self, timestamp):
		try:
			begin = time.time()
			data = self.__mt174.read()
			end = time.time()
			logging.info("Read data in %.3fs", (end - begin))
			logging.debug("Data: %s", data)
			for p in self.__processors:
				try:
					begin = time.time()
					p.process(timestamp, data)
					end = time.time()
					logging.info("Processor (%s) in %.3fs", p.getName(), (end - begin))
				except KeyboardInterrupt:
					raise
				except Exception:
					logging.exception("Error in processor")
		except KeyboardInterrupt:
			raise
		except Exception:
			logging.exception("Error in meter reader")
	
	def run(self):
		start = 0
		try:
			while True:
				while (time.time() - start) < self.__interval:
					time.sleep(Scheduler.SLEEP_TIME)
				start = time.time()
				self.execute(start)
		except KeyboardInterrupt:
			return 0
		except Exception:
			logging.exception("Error. Exiting...")
			return 1

class Processor:

	# 1-0:1.8.1*255(0001798.478*kWh)
	REGEX = re.compile("(?:\d\-\d:)?(\S+\.\S+\.\d+)(?:\*255)?\((.+)\)")
	
	def __init__(self, name):
		self.__name = name

	def getName(self):
		return self.__name

	@staticmethod
	def extract(data):
		d = dict()
		for line in data.split():
			match = Processor.REGEX.match(line)
			if match:
				d[match.group(1)] = match.group(2)
		return d
			
	def process(self, timestamp, data):
		pass

class MqttProcessor(Processor):

	MARGIN_PERCENT = 0.5
	
	MOSQUITTO_PUB = "/usr/bin/mosquitto_pub"

	def __init__(self, host, port, rootTopic, interval):
		Processor.__init__(self, "mqtt")
		self.__last = [0, (0,0,0)]
		self.__host = host
		self.__port = port
		self.__rootTopic = rootTopic
		self.__interval = interval + interval * MqttProcessor.MARGIN_PERCENT
		logging.info("Created MQTT (%s), host = %s, port = %d", self.getName(), host, port)

	def buildCommand(self, topic, value):
		command = [MqttProcessor.MOSQUITTO_PUB]
		command.extend(["-i", "%s" % self.getName()])
		command.extend(["-h", "%s" % self.__host])
		command.extend(["-p", "%d" % self.__port])
		command.extend(["-t", "%s/%s" % (self.__rootTopic, topic)])
		command.extend(["-m", "%s" % value])
		return command

	def executeCommand(self, command):
		with open("/dev/null", "w") as f:
			logging.debug("Executing: %s" % " ".join(command))
			retcode = subprocess.call(command, stdout = f, stderr = f)
			if retcode != 0:
				logging.error("Unable to execute command")
			return retcode

	def process(self, timestamp, data):
		d = Processor.extract(data)
		edis = ("1.8.0", "1.8.1", "1.8.2")
		names = ("total", "tariff1", "tariff2")
		index = [float(d[x].split("*")[0]) for x in d if x in edis]
		current = [timestamp, index]
		# current
		if self.__last[0] != 0:
			interval = current[0] - self.__last[0]
			if interval < self.__interval:
				func = lambda x,y: (x - y) * 3600. / interval
				values = map(func, current[1], self.__last[1])
				for key, value in zip(names, values):
					message = "%d %.3f" % (timestamp, value)
					topic = "current/%s" % key
					command = self.buildCommand(topic, message)
					self.executeCommand(command)
			else:
				logging.error("Interval too long: %.2f" % interval)
		# index
		for key, value in zip(names, index):
			message = "%d %.3f" % (timestamp, value)
			topic = "index/%s" % key
			command = self.buildCommand(topic, message)
			self.executeCommand(command)
			logging.info("%s = %.3f kWh", topic, value)
		# power down counter
		message = "%d %d" % (timestamp, int(d["C.7.0"]))
		topic = "powerdown/counter"
		command = self.buildCommand(topic, message)
		self.executeCommand(command)

		self.__last = current

class EibdProcessor(Processor):
	
	GROUPWRITE = "/usr/local/bin/groupwrite"

	def __init__(self, host):
		Processor.__init__(self, "eibd")
		self.__host = host
		logging.info("Created EIBD (%s), host = %s", self.getName(), host)
	
	@staticmethod
	def convertTo4Bytes(value):
		if value < 0:
			raise Exception("Value smaller than 0: %d", value)
		elif value > 2147483647:
			raise Exception("Value greater than 2147483647: %d", value)
		else:
			number = "{0:0{1}x}".format(value, 8)
			return ["0x" + number[i:i+2] for i in xrange(0, len(number), 2)]

	def buildCommand(self, groupAddress, bytes):
		command = [EibdProcessor.GROUPWRITE]
		command.append("%s" % self.__host)
		command.append("%s" % groupAddress)
		command.extend(bytes)
		return command

	def executeCommand(self, command):
		with open("/dev/null", "w") as f:
			logging.debug("Executing: %s" % " ".join(command))
			#retcode = subprocess.call(command, stdout = f, stderr = f)
			retcode = 0
			if retcode != 0:
				logging.error("Unable to execute command")
			return retcode

	def process(self, timestamp, data):
		d = Processor.extract(data)
		edis = ("1.8.0", "1.8.1", "1.8.2")
		ga = ("14/1/0", "14/1/1", "14/1/2")
		index = [float(d[x].split("*")[0]) for x in d if x in edis]
		# index
		for key, value in zip(ga, index):
			Wh = int(value * 1000)
			command = self.buildCommand(key, EibdProcessor.convertTo4Bytes(Wh))
			self.executeCommand(command)
			logging.info("%s = %d Wh", key, Wh)
		# power down counter
		key = "14/1/3"
		value = int(d["C.7.0"])
		command = self.buildCommand(key, [hex(value)])
		logging.info("%s = %d times", key, value)
		self.executeCommand(command)


class FileLogger(Processor):

	def __init__(self, filename):
		Processor.__init__(self, "file-logger")
		self.__filename = filename
		logging.info("Created FileLogger, filename = %s", filename)

	def process(self, timestamp, data):
		filename = "%s-%s.log" % (self.__filename, time.strftime("%Y-%m"))
		with open(filename, "a+") as f:
			f.write("%d: %s\n" % (timestamp, Processor.extract(data)))
			logging.debug("Written data to %s", filename)


logging.basicConfig(format = "%(levelname)s: %(message)s")
logging.getLogger().setLevel(logging.DEBUG)

interval = 60
m = MT174("/dev/ttyUSB0")
#m = FakeMeter()
#p = (Logger(),)
#p = (MqttProcessor("localhost", 1883, "/home/energy", interval), FileLogger("/tmp/data"))
p = (EibdProcessor("local:/tmp/eib"), FileLogger("/tmp/data"))
s = Scheduler(m, p, interval)
s.run()

