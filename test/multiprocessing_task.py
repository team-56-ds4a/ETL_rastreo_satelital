import time 
import multiprocessing

from utils import *
from websites import *

NUM_WORKERS = 4

start_time = time.time()

with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
    results = pool.map_async(check_website, WEBSITE_LIST)
    results.wait()

end_time = time.time()

print(f"Time Multiprocessing: {(end_time - start_time)}")