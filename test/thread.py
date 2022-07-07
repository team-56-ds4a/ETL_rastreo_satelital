import time 
from queue import Queue
from threading import Thread
from websites import *
from utils import *

NUM_WORKERS = 4
task_queue = Queue()

def worker():
    while True:
        address = task_queue.get()
        check_website(address)
        
        task_queue.task_done()
start_time = time.time()

threads = [Thread(target=worker) for _ in range(NUM_WORKERS)]

[task_queue.put(item) for item in WEBSITE_LIST]

[thread.start() for thread in threads]

task_queue.join()

end_time = time.time()

print(f"Time thread: {end_time - start_time}")