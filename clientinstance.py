import numpy
import time
from io import BytesIO
from settings import *
from threading import Thread

class ClientInstance:
    def __init__(self, MBNet, conn, addr, queue):
        self.MBNet = MBNet
        self.conn = conn
        self.addr = addr
        self.conn_start_time = 0
        self.communication_delay = 0
        self.computational_delay_list = []
        self.frame_times = []
        self.fps_list = []
        self.client_id = ' '

        self.receive_count = 0
        self.put_count = 0
        self.run_count = 0
        self.frame_drop_count = 0

        self.frame_queue = queue

    def recv_data(self):
        if not self.conn:
            raise Exception("Connection is not established")
        self.conn.sendall(b'broadcast_start')
        print("Broadcast to Device")
        self.conn_start_time = float(str(self.conn.recv(1024), 'utf-8'))
        body_size = None
        buffer = b''

        while True:
            self.receive_count=self.receive_count+1
            data = self.conn.recv(16384)

            # When connection is closed or any problem, run close code
            if not data:
                # Zero is finish flag for MobileNetTest
                self.frame_queue.put((0, 0, 0, 0))
                return
            buffer += data

            while (b'Start_Symbol' in buffer) and (b'End_Symbol' in buffer):
                start_idx = buffer.find(b'Start_Symbol')
                id_idx = buffer.find(b'Id_Symbol')
                size_idx = buffer.find(b'Size_Symbol')
                end_idx = buffer.find(b'End_Symbol')
                frame_idx = buffer.find(b'Frame_Num')

                msg_body = buffer[frame_idx+9:end_idx]
                self.client_id = buffer[start_idx + 12: id_idx].decode(errors="ignore")
                body_size = buffer[id_idx + 9:size_idx].decode(errors="ignore")
                frame_num = int(buffer[size_idx+11:frame_idx].decode(errors="ignore"))

                if body_size.isdigit():
                    body_size = int(body_size)

                if len(msg_body) == body_size:
                    self._put_frame(body_size, frame_num, msg_body)
                    buffer = buffer[end_idx+10:]

    def _put_frame(self, body_size, frame_num, msg_body):
        self.put_count = self.put_count+1
        print(self.client_id, "queue_size : ", self.frame_queue.qsize())
        if body_size and len(msg_body) == body_size:
            image = numpy.load(BytesIO(msg_body))['frame']

            # measure communication delay
            if not self.communication_delay:
                self.communication_delay = time.time() - self.conn_start_time
                ##print("start_time : ", self.conn_start_time, ",current_time : ", time.time())
                ##print(",communication : ", self.communication_delay)
            if self.frame_queue.qsize() < SERVER_QUEUE_SIZE:
                self.frame_queue.put((image, time.time(), self.client_id, frame_num))
            else:
                self.frame_drop_count+=1

    def run_test(self):
        while self.frame_queue.empty():
            continue

        while True:
            frame, start_time, id, frame_num = self.frame_queue.get()
            fps_start_time = time.time()
            if frame is 0:
                self.return_procedure()
                return

            self.run_count = self.run_count + 1
            self.MBNet.run(frame, frame_num)

            time_taken = time.time() - fps_start_time
            self.frame_times.append(time_taken)
            re_frame_times = self.frame_times[-20:]
            self.fps_list.append(len(re_frame_times) / sum(re_frame_times))

            # measure computation delay
            self.computational_delay_list.append(time.time() - start_time)
            #print(id,"s frame processing complete")

    def return_procedure(self):
        time.sleep(1)
        print("\nresult of", self.client_id)
        print("communication delay: %.4f" % (self.communication_delay))
        print("computational delay: %.4f" % (sum(self.computational_delay_list)/len(self.computational_delay_list)))
        print("Avg FPS:", sum(self.fps_list) / len(self.fps_list) )
        print("receive_count:", self.receive_count, ", put_count:", self.put_count
              , ", run_count:", self.run_count, ", frame_drop_count:", self.frame_drop_count)

    def main_task(self):
        recv_thread = Thread(target=self.recv_data)
        recv_thread.start()
        run_thread = Thread(target=self.run_test)
        run_thread.start()
        recv_thread.join()
        run_thread.join()