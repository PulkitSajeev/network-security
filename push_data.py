import os
import sys
import json
from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
print(MONGO_DB_URL)

import certifi
ca = certifi.where()

import pandas as pd
import numpy as np
import pymongo
from NetSec.exception import CustomException
from NetSec.logger import logging

class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise CustomException(e, sys)

    def csv_to_json(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise CustomException(e, sys)

    def push_data_to_mongo(self, records, database, collection):
        try:
            self.database = database
            self.collection = collection
            self.records = records
            self.client = pymongo.MongoClient(MONGO_DB_URL, tlsCAFile=ca)
            self.database = self.client[self.database]
            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)
            return (len(self.records), "Records inserted successfully")
        except Exception as e:
            raise CustomException(e, sys)

if __name__ == '__main__':
    FILE_PATH = "data/phisingData.csv"
    DATABASE = "pulkitsajeev"
    Collection = "NetworkData"
    network_obj = NetworkDataExtract()
    records = network_obj.csv_to_json(file_path = FILE_PATH)
    print(records)
    no_of_records = network_obj.push_data_to_mongo(records = records, database = DATABASE, collection = Collection)
    print(no_of_records)