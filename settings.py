# Need to change on Both part
SERVER_HOST = '192.168.1.9'
SERVER_PORT = 10001
AWS_IOT_GREENGRASS_ENDPOINT = r'adb3j4sb3x8on-ats.iot.us-west-2.amazonaws.com'
MODE = r'both'
TOPIC = r'message/ready'
DIR_ROOTCA = r'certification/root-ca-cert.pem'
DIR_CERT = r'certification/b23fc3ba4f.cert.pem'     # *
DIR_KEY = r'certification/b23fc3ba4f.private.key'   # *
THING_NAME = r'Unsang_Laptop'                       # *
MESSAGE = THING_NAME + r" BROADCAST"
NUMBER_OF_FRAME = 460

# Need to change on Client part
CLIENT_ID = THING_NAME                                # *
NUMBER_OF_SEND_VIDEO = 1
RATE_OF_SENDING_PART = 20

# Need to change on Server part
SERVER_QUEUE_SIZE = 50                              # *
NUMBER_OF_CLIENT = 1                                # *
NUMBER_OF_VIDEOS_EACH_CLIENT = 1                     # *
CLIENT1_HOST = "192.168.1.16"
CLIENT2_HOST = "192.168.1.11"
CLIENT3_HOST = "192.168.1.2"
CLIENT4_HOST = "192.168.1.3"
CLIENT_PORT = 8883
















































NUMBER_OF_TOTAL_FRAME = 461
VIDEO_PATH = r'video/Pexels Videos 1466210.mp4'
INPUT_SHAPE = (300, 300, 3)
WEIGHT_PATH = r'model/mobilenetssd300.hdf5'
CLASS_NAMES = ["background", "aeroplane", "bicycle", "bird", "boat", "bottle", "bus", "car", "cat", "chair", "cow",
               "diningtable", "dog", "horse", "motorbike", "person", "pottedplant", "sheep", "sofa", "train",
               "tvmonitor"]
DETECTION_LIST = ['person', 'chair', 'boat']


