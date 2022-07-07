import os 
import time
import threading
import multiprocessing

NUM_WORKERS = 4

def only_sleep():
    """hacer nada, esperar a que acabe el temporizador """
    print(f"PID:{os.getpid()}, Process Name: {multiprocessing.current_process().name}, Thread Name: {threading.current_thread().name}")
    time.sleep(1)
    
def crunch_numbers():
    """Realizar algunos calculos"""
    print(f"PID:{os.getpid()}, Process Name: {multiprocessing.current_process().name}, Thread Name: {threading.current_thread().name}")
    x = 0
    while x < 10000000:
        x += 1
      
#SERIE  
start_time = time.time()
for _ in range(NUM_WORKERS):
    crunch_numbers()
end_time = time.time()

print(f"Serial Time = {end_time - start_time}")

#THREADS
start_time = time.time()
threads = [threading.Thread(target=crunch_numbers) for _ in range(NUM_WORKERS)]
[thread.start() for thread in threads]
[thread.join() for thread in threads]
end_time = time.time()

print(f"Threads Time = {end_time - start_time}")

#PARALLEL
start_time = time.time()
processes = [multiprocessing.Process(target=crunch_numbers) for _ in range(NUM_WORKERS)]
[process.start() for process in processes]
[process.join() for process in processes]
end_time = time.time()

print(f"Parallel Time = {end_time - start_time}")

