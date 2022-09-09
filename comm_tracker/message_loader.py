'''
    MessageLoader - class for loadingrecords
    BJB 8/25/22
'''
import datetime
import json
import os
import sys
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
import os
import time
from confluent_kafka import Producer
import socket
import json
base_dir = os.path.dirname(os.path.abspath(__file__))

sys.path.append(os.path.dirname(base_dir))
sys.path.append(os.path.join(base_dir, "templates"))
from bbutil import Util

class MessageLoader:

    def __init__(self, topic, details={}):
        self.bulk_docs = []
        self.counter = 0
        self.settings = details["settings"]
        # self.conn = self.pubsub_connection()
        self.batch_size = self.settings["batch_size"]
        self.project = self.settings["gcp"]["pub_sub_project"]
        # self.topic = self.settings["gcp"]["pub_sub_topic"]
        self.timeout = self.settings["gcp"]["pub_sub_timeout"]
        # self.logit(f'Publisher set in {self.project} for topic: {self.topic}')

        self.topic = topic
        self.producer = Producer(self.settings["confluent"])


# Best practice for higher availability in librdkafka clients prior to 1.7
# session.timeout.ms=45000

# Required connection configs for Confluent Cloud Schema Registry
# schema.registry.url=https://{{ SR_ENDPOINT }}
# basic.auth.credentials.source=USER_INFO
# basic.auth.user.info={{ SR_API_KEY }}:{{ SR_API_SECRET }}

    def __del__(self):
        cool = "not"
        # self.conn.close()

    def not_add(self, doc):
        if len(self.bulk_docs) == self.batch_size:
            self.flush()
        self.bulk_docs.append(doc)
        self.counter += 1

    def add_pubsub(self, payload):
        topic_path = self.conn.topic_path(self.project, self.topic)
        data = json.dumps(payload).encode("utf-8")
        future = self.conn.publish(topic_path, data=data)
        self.logit("Pushed message to topic.")

    def add_kafka(self, payload):
        data = json.dumps(payload).encode("utf-8")
        self.producer.produce(self.topic, data, callback=self.delivery_callback)
        self.producer.poll()
        # self.producer.flush()
        
    def delivery_callback(self, err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        #else:
        #     print("Produced event to topic ", self.topic)

    def flush(self):
        cool = "not"
        self.producer.flush()

        '''
        db = self.conn[self.settings["database"]]
        ans = db[self.settings["collection"]].insert_many(self.bulk_docs)
        self.logit(f"Loading batch: {self.batch_size} - total: {self.counter}")
        self.bulk_docs = []
        '''

    def pubsub_connection(self, type="uri"):
        publisher = pubsub_v1.PublisherClient()
        return publisher

    def logit(self, message, log_type="INFO", display_only=True):
        cur_date = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        stamp = f"{cur_date}|{log_type}> "
        if type(message) == dict or type(message) == list:
            message = str(message)
        if log_type == "raw":
            message = "Raw output, check file"
        for line in message.splitlines():
            print(f"{stamp}{line}")

