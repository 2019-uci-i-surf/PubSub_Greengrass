import numpy
from socket import socket, AF_INET, SOCK_STREAM
from io import BytesIO
from settings import *
import cv2
import time
from queue import Queue
import math

class CameraClient:
    def __init__(self):
        self.socket = socket(AF_INET, SOCK_STREAM)
        self.time_list=[]
        self.frame_rate=[]
        self.wait_send_queue = Queue()
        self.number_of_sent_frame = 0
        self.start_send_time = 0

    def connect_to_server(self, host, port):
        print('try to connect to server..')
        self.socket.connect((host, port))
        print('successfully connected to server')

    def put_frame(self, video_path):
        vidcap = cv2.VideoCapture(video_path)
        count = 0
        msg = str(self.socket.recv(1024), 'utf-8') # receive data(broadcast or start) from server
        if msg == 'broadcast_start':
            self.socket.sendall(bytes(str(time.time()), encoding='utf-8'))
            while True:
                success, image = vidcap.read()
                if not success:
                    print('video is not opened!')
                    break
                count += 1
                bytes_io = BytesIO()
                numpy.savez_compressed(bytes_io, frame=image)
                bytes_io.seek(0)
                bytes_image = bytes_io.read() # byte per 1frame

                msg = ('Start_Symbol' + CLIENT_ID + 'Id_Symbol' + str(len(bytes_image)) + 'Size_Symbol' + str(count) + 'Frame_Num').encode() + bytes_image + ('End_Symbol').encode()
                self.wait_send_queue.put(msg)

    def get_frame(self):
        while self.wait_send_queue.empty():
            continue
        # start get frame from queue
        self.start_send_time = time.time()

        count = RATE_OF_SENDING_PART
        last_time = time.time()
        while True:
            current_time = time.time()
            if current_time > last_time + 1:
                last_time = current_time
                count = round(count + RATE_OF_SENDING_PART, 3)
                print("count : ", count)
            if count >= 1:
                self.send_frame()
                count = round(count-1, 3)

            if self.number_of_sent_frame == NUMBER_OF_TOTAL_FRAME:
                self.result()
                break

    def send_frame(self):
        frame = self.wait_send_queue.get()
        self.socket.sendall(frame)
        self.number_of_sent_frame += 1
        print(CLIENT_ID, "sent frame : ", self.number_of_sent_frame)

    def result(self):
        run_time = time.time()-self.start_send_time
        print("\nSending of", CLIENT_ID, "complete")
        print(self.number_of_sent_frame, "frames are sent.")
        print("run time : ", run_time)
        print("Avg Frame rate of sending:", self.number_of_sent_frame / run_time)

    @staticmethod
    def mp_routine(host, port):
        cc = CameraClient()
        cc.connect_to_server(host=host, port=port)
        cc.put_frame(video_path=VIDEO_PATH)
        cc.get_frame()