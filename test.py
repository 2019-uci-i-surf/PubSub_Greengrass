from multiprocessing import Process, Queue

q = Queue()

def put_1(q):
    a=1
    while(1):
        a+=1
        q.put(a)

def get_1(q):
    while(1):
        print(q.get())

if __name__ == '__main__':
    proc1 = Process(target=put_1, args=(q,))
    proc1.start()

    proc2 = Process(target=get_1, args=(q,))
    proc2.start()

    proc1.join()
    proc2.join()

