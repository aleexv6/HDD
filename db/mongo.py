from pymongo import MongoClient

class MongoWrapper:
    def __init__(self, uri, db_name):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def collection(self, name):
        return self.db[name]