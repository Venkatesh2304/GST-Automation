from pymongo import MongoClient
try:
    conn = MongoClient()
    print("Connected successfully!!!")
except:  
    raise Exception("Could not connect to MongoDB")