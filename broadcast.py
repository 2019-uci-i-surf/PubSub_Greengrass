import numpy
from io import BytesIO
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
from settings import *
from queue import Queue
from mobilenettest import MobileNetTest
import base64


start_time11 = time.time()

AllowedActions = ['both', 'publish', 'subscribe']

# General message notification callback
count=0
cur_frame=1
single_frame=b""

def customOnMessage(message):
    get_message(message.payload)
    # print("\n\n\n ----------------------------------------------------------------------------------------------------\n"
    #       "   Received message on topic %s: %s\n"
    #       " ----------------------------------------------------------------------------------------------------\n"
    #       % (message.topic, message.payload))

def get_message(client_message):
    global count
    count = count
    # print("first message : ", sys.getsizeof(client_message))
    # print("asadasdasd : ", type(client_message))
    # print("asdasdasda : ", client_message)

    client_id_idx = client_message.find(b"client_id")
    data_size_idx = client_message.find(b"data_size")
    frame_num_idx = client_message.find(b"frame_num")
    frame_data_idx = client_message.find(b"frame_data")
    packet_end_idx = client_message.find(b"packet_end")

    client_id = client_message[client_id_idx+10:data_size_idx].decode()
    data_size = int(client_message[data_size_idx+10:frame_num_idx].decode())
    frame_num = int(client_message[frame_num_idx+10:frame_data_idx].decode())
    frame_data = client_message[frame_data_idx+11:packet_end_idx]

    # print("2 : ", client_id)
    # print("3 : ", data_size)
    # print("4 : ", frame_num)
    #
    # print("5 : ", frame_data)
    # print("5555555 : ", type(frame_data))
    #
    if frame_data == b"READY":
        count = count + 1
        print("   Receive ready message from", frame_data)
    else:
        if frame_num != 0:
            make_frame(frame_num, frame_data)




def make_frame(frame_num, frame_data):
    global cur_frame
    cur_frame = cur_frame
    global single_frame
    single_frame = single_frame
    if frame_num == cur_frame:
        single_frame+=frame_data
    else:
        image = numpy.load(BytesIO(single_frame))['frame']
        #print("size : ", len(image), image)

        run_mobilenet(image, cur_frame)

        cur_frame = frame_num
        single_frame = frame_data

def run_mobilenet(frame, cur_frame):
    myAWSIoTMQTTClient2.mobile_net.run(frame, cur_frame)
    #print("frame num: ", cur_frame)
    #print(frame)

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
#myAWSIoTMQTTClient1 = AWSIoTMQTTClient(clientId)
myAWSIoTMQTTClient2 = AWSIoTMQTTClient(clientId)
#myAWSIoTMQTTClient3 = AWSIoTMQTTClient(clientId)
#myAWSIoTMQTTClient4 = AWSIoTMQTTClient(clientId)

myAWSIoTMQTTClient2.mobile_net = MobileNetTest(CLASS_NAMES, WEIGHT_PATH, INPUT_SHAPE)

#myAWSIoTMQTTClient1.configureCredentials(groupCA, privateKeyPath, certificatePath)
myAWSIoTMQTTClient2.configureCredentials(groupCA, privateKeyPath, certificatePath)
#myAWSIoTMQTTClient3.configureCredentials(groupCA, privateKeyPath, certificatePath)
#myAWSIoTMQTTClient4.configureCredentials(groupCA, privateKeyPath, certificatePath)

#myAWSIoTMQTTClient1.onMessage = customOnMessage
myAWSIoTMQTTClient2.onMessage = customOnMessage
#myAWSIoTMQTTClient3.onMessage = customOnMessage
#myAWSIoTMQTTClient4.onMessage = customOnMessage

connected = False
for connectivityInfo in coreInfo.connectivityInfoList:
    #currentHost = connectivityInfo.host
    #currentPort = connectivityInfo.port

    currentPort = 8883
    print("Trying to connect to core at %s:%d" % (CLIENT1_HOST, currentPort))
    #myAWSIoTMQTTClient1.configureEndpoint(CLIENT1_HOST, currentPort)

    print("Trying to connect to core at %s:%d" % (CLIENT2_HOST, currentPort))
    myAWSIoTMQTTClient2.configureEndpoint(CLIENT2_HOST, currentPort)

    print("Trying to connect to core at %s:%d" % (CLIENT3_HOST, currentPort))
    #myAWSIoTMQTTClient3.configureEndpoint(CLIENT3_HOST, currentPort)

    print("Trying to connect to core at %s:%d" % (CLIENT4_HOST, currentPort))
    #myAWSIoTMQTTClient4.configureEndpoint(CLIENT4_HOST, currentPort)

    try:
        #myAWSIoTMQTTClient1.connect()
        myAWSIoTMQTTClient2.connect()
        #myAWSIoTMQTTClient3.connect()
        #myAWSIoTMQTTClient4.connect()

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
    #myAWSIoTMQTTClient1.subscribe(topic, 0, None)
    myAWSIoTMQTTClient2.subscribe(topic, 0, None)
    #myAWSIoTMQTTClient3.subscribe(topic, 0, None)
    #myAWSIoTMQTTClient4.subscribe(topic, 0, None)
time.sleep(2)
print("\n\n\n ----------------------------------------------------------------------------------------------------\n"
      "                                      Ready to accept client's message"
      "\n ----------------------------------------------------------------------------------------------------\n")
while 1:
    if count == NUMBER_OF_CLIENT:
        print("\n ----------------------------------------------------------------------------------------------------\n"
              "   Successful connection with clients"
              "\n ----------------------------------------------------------------------------------------------------\n")
        time.sleep(3)
        break

loopCount = 0

if MODE == 'both' or MODE == 'publish':
    message = {}
    message['message'] = MESSAGE
    message['sequence'] = loopCount
    messageJson = json.dumps(message)
    print(type(messageJson))
    #myAWSIoTMQTTClient1.publish(topic, messageJson, 0)
    myAWSIoTMQTTClient2.publish(topic, messageJson, 0)
    #myAWSIoTMQTTClient3.publish(topic, messageJson, 0)
    #myAWSIoTMQTTClient4.publish(topic, messageJson, 0)
    print('Published topic %s: %s\n' % (topic, messageJson))
    loopCount += 1

#run server.py
print("\n -----------------------------------------------------------------------------------------"
      "\n   Broadcast to clients \n   %s: %s"
      "\n -----------------------------------------------------------------------------------------"
      "\n   Run server.py"
      "\n -----------------------------------------------------------------------------------------\n"
      % (topic, messageJson))

print("\n -----------------------------------------------------------------------------------------")

a=1
while 1:
    if cur_frame>=a:
        a+=1
        print(cur_frame, "frame running time : ", time.time()-start_time11)
    if cur_frame==461:
        break
