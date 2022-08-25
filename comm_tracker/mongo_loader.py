'''
    MongoLoader - class for loadingrecords
    BJB 8/25/22
'''
import datetime
import json
import os
import sys
from pymongo import MongoClient

class DbLoader:

    def __init__(self, details = {}):
        self.bulk_docs = []
        self.counter = 0
        self.settings = details["settings"]
        self.conn = self.client_connection()
        self.batch_size = self.settings["batch_size"]
    
    def __del__(self):
        self.conn.close()

    def add(self, doc):
        if len(self.bulk_docs) == self.batch_size:
            self.flush()
        self.bulk_docs.append(doc)
        self.counter += 1

    def flush(self):
        db = self.conn[self.settings["database"]]
        ans = db[self.settings["collection"]].insert_many(self.bulk_docs)
        self.logit(f"Loading batch: {self.batch_size} - total: {self.counter}")
        self.bulk_docs = []
        

    def client_connection(self, type = "uri"):
        mdb_conn = self.settings[type]
        username = self.settings["username"]
        password = self.settings["password"]
        mdb_conn = mdb_conn.replace("//", f'//{username}:{password}@')
        self.logit(f'Connecting: {mdb_conn}')
        client = MongoClient(mdb_conn)
        return client


    def logit(self, message, log_type = "INFO", display_only = True):
        cur_date = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        stamp = f"{cur_date}|{log_type}> "
        if type(message) == dict or type(message) == list:
            message = str(message)
        if log_type == "raw":
            message = "Raw output, check file"
        for line in message.splitlines():
            print(f"{stamp}{line}")
