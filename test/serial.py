# serial_squirrel.py
from websites import *
from utils import *
 
import time
 
 
start_time = time.time()
 
for address in WEBSITE_LIST:
    check_website(address)
         
end_time = time.time()        
 
print(f"Time  Serial: {end_time - start_time}")