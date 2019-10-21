# import the necessary packages

from settings import *
from multiprocessing import Process
from cameraclient import CameraClient

if __name__ == '__main__':
    procs = list()
    for process_number in range(NUMBER_OF_SEND_VIDEO):
        proc = Process(target=CameraClient.mp_routine, args=(SERVER_HOST, SERVER_PORT))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()
