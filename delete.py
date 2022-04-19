import threading
import multiprocessing
import numpy as np
import os,time
from pymongo import MongoClient
 
 
myclient = MongoClient("mongodb://localhost:27017/")
 
# database
db = myclient["powned"]
 
# Created or Switched to collection
# names: GeeksForGeeks
collection = db["users"]
print(collection.count_documents({}))
