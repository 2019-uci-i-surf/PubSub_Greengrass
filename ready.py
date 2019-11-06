import os
import sys
import time
import uuid
import json
import logging
from AWSIoTPythonSDK.core.greengrass.discovery.providers import DiscoveryInfoProvider
from AWSIoTPythonSDK.core.protocol.connection.cores import ProgressiveBackOffCore
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from AWSIoTPythonSDK.exception.AWSIoTExceptions import DiscoveryInvalidRequestException
import client
import json
from settings import *
import cv2
import numpy
from io import BytesIO
from queue import Queue


AllowedActions = ['both', 'publish', 'subscribe']

# General message notification callback
broadcast=0
def customOnMessage(message):
    global broadcast
    broadcast=1
    print("\n\n\n ----------------------------------------------------------------------------------------------------\n"
          "   Received message on topic %s: %s\n"
          " ----------------------------------------------------------------------------------------------------\n"
          % (message.topic, message.payload))

MAX_DISCOVERY_RETRIES = 10
GROUP_CA_PATH = "./groupCA/"

host = AWS_IOT_GREENGRASS_ENDPOINT
rootCAPath = DIR_ROOTCA
certificatePath = DIR_CERT
privateKeyPath = DIR_KEY
clientId = THING_NAME
thingName = THING_NAME
topic = TOPIC

# Configure logging
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.DEBUG)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

# Progressive back off core
backOffCore = ProgressiveBackOffCore()

# Discover GGCs
discoveryInfoProvider = DiscoveryInfoProvider()
discoveryInfoProvider.configureEndpoint(host)
discoveryInfoProvider.configureCredentials(rootCAPath, certificatePath, privateKeyPath)
discoveryInfoProvider.configureTimeout(10)  # 10 sec

retryCount = MAX_DISCOVERY_RETRIES
discovered = False
groupCA = None
coreInfo = None
while retryCount != 0:
    try:
        discoveryInfo = discoveryInfoProvider.discover(thingName)
        caList = discoveryInfo.getAllCas()
        coreList = discoveryInfo.getAllCores()

        # We only pick the first ca and core info
        groupId, ca = caList[0]
        coreInfo = coreList[0]
        print("Discovered GGC: %s from Group: %s" % (coreInfo.coreThingArn, groupId))

        print("Now we persist the connectivity/identity information...")
        groupCA = GROUP_CA_PATH + groupId + "_CA_" + str(uuid.uuid4()) + ".crt"
        if not os.path.exists(GROUP_CA_PATH):
            os.makedirs(GROUP_CA_PATH)
        groupCAFile = open(groupCA, "w")
        groupCAFile.write(ca)
        groupCAFile.close()

        discovered = True
        print("Now proceed to the connecting flow...")
        break
    except DiscoveryInvalidRequestException as e:
        print("Invalid discovery request detected!")
        print("Type: %s" % str(type(e)))
        print("Error message: %s" % e.message)
        print("Stopping...")
        break
    except BaseException as e:
        print("Error in discovery!")
        print("Type: %s" % str(type(e)))
        print("Error message: %s" % e.message)
        retryCount -= 1
        print("\n%d/%d retries left\n" % (retryCount, MAX_DISCOVERY_RETRIES))
        print("Backing off...\n")
        backOffCore.backOff()

if not discovered:
    print("Discovery failed after %d retries. Exiting...\n" % (MAX_DISCOVERY_RETRIES))
    sys.exit(-1)

# Iterate through all connection options for the core and use the first successful one
myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
myAWSIoTMQTTClient.configureCredentials(groupCA, privateKeyPath, certificatePath)
myAWSIoTMQTTClient.onMessage = customOnMessage

connected = False
for connectivityInfo in coreInfo.connectivityInfoList:
    currentHost = "127.0.0.1"
    currentPort = 8883

    print("Trying to connect to core at %s:%d" % (currentHost, currentPort))
    myAWSIoTMQTTClient.configureEndpoint(currentHost, currentPort)
    try:
        myAWSIoTMQTTClient.connect()
        connected = True
        break
    except BaseException as e:
        print("Error in connect!")
        print("Type: %s" % str(type(e)))
        print("Error message: %s" % e.message)

if not connected:
    print("Cannot connect to core %s. Exiting..." % coreInfo.coreThingArn)
    sys.exit(-2)

# Successfully connected to the core
if MODE == 'both' or MODE == 'subscribe':
    myAWSIoTMQTTClient.subscribe(topic, 0, None)
time.sleep(2)

loopCount = 0


if MODE == 'both' or MODE == 'publish':
    message = {}
    message['message'] = MESSAGE
    message['device'] = THING_NAME
    messageJson = json.dumps(message)
    myAWSIoTMQTTClient.publish(topic, messageJson, 0)
    print('Published topic %s: %s\n' % (topic, messageJson))

while 1:
    if(broadcast==1):
        break

print("   Start Send Video\n"
      " ----------------------------------------------------------------------------------------------------\n")
time.sleep(5)

wait_send_queue = Queue()
number_of_sent_frame = 0

def put_frame():
    sent_count = 0
    vidcap = cv2.VideoCapture(VIDEO_PATH)
    while True:
        success, image = vidcap.read()
        if not success:
            print('video is not opened!')
            break
        sent_count += 1
        bytes_io = BytesIO()
        numpy.savez_compressed(bytes_io, frame=image)
        bytes_io.seek(0)
        bytes_image = bytes_io.read()  # byte per 1frame

        msg = ('Start_Symbol' + CLIENT_ID + 'Id_Symbol' + str(len(bytes_image)) + 'Size_Symbol' + str(sent_count) + 'Frame_Num').encode() + bytes_image + ('End_Symbol').encode()
        wait_send_queue.put(msg)

def get_frame():
    while wait_send_queue.empty():
        continue
    # start get frame from queue
    start_send_time = time.time()

    rate_count = RATE_OF_SENDING_PART
    last_time = time.time()
    while True:
        current_time = time.time()
        if current_time > last_time + 1:
            last_time = current_time
            rate_count = round(rate_count + RATE_OF_SENDING_PART, 3)
        if rate_count >= 1:
            send_frame()
            rate_count = round(rate_count-1, 3)
        #if number_of_sent_frame == NUMBER_OF_TOTAL_FRAME:
        #    result()

def send_frame():
    frame = wait_send_queue.get()

    msg = {}
    msg['message'] = frame
    messageJson = json.dumps(msg)
    myAWSIoTMQTTClient.publish(TOPIC, messageJson, 0)

    #number_of_sent_frame += 1
    #print(CLIENT_ID, "sent frame : ", number_of_sent_frame)
'''
def result():
    run_time = time.time()-start_send_time
    print("\nSending of", CLIENT_ID, "complete")
    print(self.number_of_sent_frame, "frames are sent.")
    print("run time : ", run_time)
    print("Avg Frame rate of sending:", self.number_of_sent_frame / run_time)
'''

put_frame()
get_frame()
