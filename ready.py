import os
import sys
import uuid
import logging
from AWSIoTPythonSDK.core.greengrass.discovery.providers import DiscoveryInfoProvider
from AWSIoTPythonSDK.core.protocol.connection.cores import ProgressiveBackOffCore
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from AWSIoTPythonSDK.exception.AWSIoTExceptions import DiscoveryInvalidRequestException
import time
import json
from settings import *
import cv2
import numpy
import base64
from io import BytesIO
from multiprocessing import Process, Queue

start_time11 = time.time()



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
    send_data = bytearray(("client_id:" + CLIENT_ID + "data_size:" + str(0) \
                + "frame_num:" + str(0) + "frame_data:" + MESSAGE + "packet_end").encode())
    myAWSIoTMQTTClient.publish(topic, send_data, 0)
    print('Published topic %s: %s\n' % (topic, send_data))

while 1:
    if(broadcast==1):
        break

print("   Start Send Video\n"
      " ----------------------------------------------------------------------------------------------------\n")
time.sleep(5)

def put_frame():
    put_count = 0
    vidcap = cv2.VideoCapture(VIDEO_PATH)
    send_count=0
    while True:
        success, image = vidcap.read()
        if success != True:
            break
        put_count += 1

        bytes_io = BytesIO()
        numpy.savez_compressed(bytes_io, frame=image)
        bytes_io.seek(0)
        bytes_image = bytes_io.read()  # byte per 1frame
        send_count+=1
        size=0
        while len(bytes_image) > 0:
            sep_data = bytes_image[0:100000]
            bytes_image = bytes_image[100000:]

            # msg = {}
            # msg["client_id"] = CLIENT_ID
            # msg["data_size"] = len(sep_data)
            # msg["frame_num"] = send_count
            # msg["frame_data"] = send_data
            # msg["packet_end"] = ""

            #messageJson = json.dumps(msg)
            #print("messageJson size : ", sys.getsizeof(messageJson))
            #print("messageJson type : ", type(messageJson))
            send_data = bytearray(("client_id:" + CLIENT_ID + "data_size:" + str(len(sep_data)) \
                        + "frame_num:" + str(send_count) + "frame_data:").encode()) + bytearray(sep_data) + bytearray(("packet_end").encode())
            myAWSIoTMQTTClient.publish(topic, send_data, 0)
            print("send_count : ", send_count)



        #msg = ('Start_Symbol' + CLIENT_ID + 'Id_Symbol' + str(len(bytes_image)) + 'Size_Symbol' + str(put_count) + 'Frame_Num').encode() + bytes_image + ('End_Symbol').encode()
        #print(sent_count)
        #print(type(msg))
        #print(byte_array)
        #print()

        print("qsize : ", wait_send_queue.qsize(), "put_count : ", put_count)





wait_send_queue = Queue()
#number_of_sent_frame = 0

put_frame()

print("running time : ", time.time() - start_time11)

while 1:
    continue

