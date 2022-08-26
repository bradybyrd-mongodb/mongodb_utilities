'''
    MongoLoader - class for loadingrecords
    BJB 8/25/22
'''
import datetime
import json
import os
import sys
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

class MessageLoader:

    def __init__(self, details = {}):
        self.bulk_docs = []
        self.counter = 0
        self.settings = details["settings"]
        self.conn = self.pubsub_connection()
        self.batch_size = self.settings["batch_size"]
        self.project = self.settings["gcp"]["pub_sub_project"]
        self.topic = self.settings["gcp"]["pub_sub_topic"]
        self.timeout = self.settings["gcp"]["pub_sub_timeout"]
        self.logit(f'Publisher set in {self.project} for topic: {self.topic}')
    
    def __del__(self):
        cool = "not"
        #self.conn.close()

    def not_add(self, doc):
        if len(self.bulk_docs) == self.batch_size:
            self.flush()
        self.bulk_docs.append(doc)
        self.counter += 1

    def add(self, payload):        
            topic_path = self.conn.topic_path(self.project, self.topic)        
            data = json.dumps(payload).encode("utf-8")           
            future = self.conn.publish(topic_path, data=data)
            self.logit("Pushed message to topic.")
            
    def flush(self):
        cool = "not"
        '''
        db = self.conn[self.settings["database"]]
        ans = db[self.settings["collection"]].insert_many(self.bulk_docs)
        self.logit(f"Loading batch: {self.batch_size} - total: {self.counter}")
        self.bulk_docs = []
        '''

    def pubsub_connection(self, type = "uri"):
        publisher = pubsub_v1.PublisherClient()
        return publisher


    def logit(self, message, log_type = "INFO", display_only = True):
        cur_date = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        stamp = f"{cur_date}|{log_type}> "
        if type(message) == dict or type(message) == list:
            message = str(message)
        if log_type == "raw":
            message = "Raw output, check file"
        for line in message.splitlines():
            print(f"{stamp}{line}")
