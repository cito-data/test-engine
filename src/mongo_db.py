from pymongo import MongoClient
from config import getMongoDetails

def get_mongo_connection():
    details = getMongoDetails()
    client = MongoClient(details[1])
    print("Successfully connected to MongoDb")

    if not details[0]:
        raise Exception("Cannot have undefined database name")
    
    return client[details[0]]