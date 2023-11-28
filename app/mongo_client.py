from pymongo import MongoClient
from app.mongo_config import mongoServer, databaseName


def getClient():
    client = MongoClient(mongoServer)
    return client


def getDatabase():
    client = getClient()
    db = client[databaseName]
    return db


def getCollection(collectionName):
    db = getDatabase()
    return db[collectionName]
