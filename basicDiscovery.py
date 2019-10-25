# /*
# * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# *
# * Licensed under the Apache License, Version 2.0 (the "License").
# * You may not use this file except in compliance with the License.
# * A copy of the License is located at
# *
# *  http://aws.amazon.com/apache2.0
# *
# * or in the "license" file accompanying this file. This file is distributed
# * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# * express or implied. See the License for the specific language governing
# * permissions and limitations under the License.
# */


import os
import sys
import time
import uuid
import json
import logging
import argparse
from AWSIoTPythonSDK.core.greengrass.discovery.providers import DiscoveryInfoProvider
from AWSIoTPythonSDK.core.protocol.connection.cores import ProgressiveBackOffCore
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from AWSIoTPythonSDK.exception.AWSIoTExceptions import DiscoveryInvalidRequestException
from settings import *
from server import run_server

AllowedActions = ['both', 'publish', 'subscribe']

# General message notification callback
count=0
def customOnMessage(message):
    global count
    count += 1
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
myAWSIoTMQTTClient1 = AWSIoTMQTTClient(clientId)
myAWSIoTMQTTClient1.configureCredentials(groupCA, privateKeyPath, certificatePath)
myAWSIoTMQTTClient1.onMessage = customOnMessage

connected = False
for connectivityInfo in coreInfo.connectivityInfoList:
    currentHost = connectivityInfo.host
    currentPort = connectivityInfo.port

    currentHost = "192.168.1.4"
    currentPort = 8883
    print("Trying to connect to core at %s:%d" % (currentHost, currentPort))
    myAWSIoTMQTTClient1.configureEndpoint(currentHost, currentPort)
    try:
        myAWSIoTMQTTClient1.connect()
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
    myAWSIoTMQTTClient1.subscribe(topic, 0, None)
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
    myAWSIoTMQTTClient1.publish(topic, messageJson, 0)
    if MODE == 'publish':
        print('Published topic %s: %s\n' % (topic, messageJson))
    loopCount += 1

#run server.py
print("\n -----------------------------------------------------------------------------------------"
      "\n   Broadcast to clients \n   %s: %s"
      "\n -----------------------------------------------------------------------------------------"
      "\n   Run server.py"
      "\n -----------------------------------------------------------------------------------------\n"
      % (topic, messageJson))

run_server()

