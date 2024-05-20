#!/usr/bin/env python

########################################################################
#
# Many of you like to get fancy by creating separate object classes
# and external file dependencies, e.g. json files,
# I discourage you from doing that because there are file path
# reference issues that make things difficult when you containerize
# and deploy to gke. Try to keep everything in this 1 file.
# The only exception to this rule are faker models which need to be
# pre-built and tested and checked in.
#
########################################################################

# Allows us to make many pymongo requests in parallel to overcome the single threaded problem
import gevent
from gevent import monkey
_ = gevent.monkey.patch_all()

########################################################################
# Add any additional imports here.
# But make sure to include in requirements.txt
########################################################################
import pymongo
from bson import json_util
from bson.json_util import loads
from bson import ObjectId
from locust import User, events, task, constant, tag, between
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
from pymongo import UpdateMany
import time
from pickle import TRUE
from datetime import datetime, timedelta
#import datetime
import random
#from faker import Faker
import pprint

########################################################################
# Global Static Variables that can be accessed without referencing self
# Change the connection string to point to the correct db
# and double check the readpreference etc.
########################################################################
client = "me" #None
coll = None
# Log all application exceptions (and audits) to the same cluster
audit = None
version = None
#fake = Faker()

# docs to insert per batch insert
batch_size = 1000
settings = {
    "uri": "mongodb+srv://claims-demo.vmwqj.mongodb.net",
    "database": "building_monitor",
    "collection": "readings",
    "base_id" : 1000000,
    "count_coll" : "counts",
    "batch_size": 1000,
    "username": "main_admin",
    "password": "<secret>",
    "version" : "3.1"
}
########################################################################
# Even though locust is designed for concurrency of simulated users,
# given how resource intensive fakers/bulk inserts are,
# you should only run 1 simulated user / worker else you'll kill the
# CPU of the workers.
########################################################################
class MetricsLocust(User):
    ####################################################################
    # Unlike a standard locust file where we throttle requests on a per
    # second basis, since we are trying to load data asap, there will
    # be no throttling
    ####################################################################

    def __init__(self, parent):
        super().__init__(parent)

        global client, coll, audit, auditcoll, batch_size, settings, version
        database = settings["database"] 
        collection = settings["collection"]
        srv = settings["uri"]
        batch_size = settings["batch_size"]
        version = settings["version"]
        # Singleton
        if (client is None):
            # Parse out env variables from the host
            vars = self.host.split("|")
            srv = vars[0]           
            db = client[vars[1]]
            coll = db[vars[2]]
            # docs to insert per batch insert
            batch_size = int(vars[3])
            version = "1.0"
            if len(vars) > 4:
                version = vars[4]

        print("SRV:",srv)
        client = pymongo.MongoClient(srv)       
        db = client[database]
        settings["db"] = db
        coll = db[collection]
        auditcoll = db["audit"]
        # Log all application exceptions (and audits) to the same cluster
        audit = client.mlocust.audit
        print("Batch size from Host:",batch_size)
        self.audit("init", f'Starting Params: C:{database}.{collection}, B:{batch_size}, V:{version}')

    ################################################################
    # Example helper function that is not a Locust task.
    # All Locust tasks require the @task annotation
    # You have to pass the self reference for all helper functions
    ################################################################
    def get_time(self):
        return time.time()

    ################################################################
    # Audit should only be intended for logging errors
    # Otherwise, it impacts the load on your cluster since it's
    # extra work that needs to be performed on your cluster
    ################################################################
    def audit(self, type, msg):
        print("Audit: ", msg)
        audit.insert_one({"type":type, "ts":datetime.now(), "version": version, "msg":str(msg)})


    ################################################################
    # We need to simulate polling update
    # this method will update
    # Strategy:
    #  
    ################################################################
    def generate_measurement(self, id_cnt):
        '''
            Generate updates to add to measurement array
        '''
        dps = []
        cur = datetime.now()
        device_id = random.randint(1000,9999)
        device_details = self.device_type(device_id)
        rlow = device_details["avg"] * 10
        rhigh = int(device_details["avg"] * 10 * 1.1)
        for k in range(5):
            dps.append(random.randint(rlow,rhigh)/10)
        pipe = {"$addToSet" : {"dataPoints" : {"$each" : dps}},"$inc" : {"pointCount" : 5}, "$set" : { "lastPoint" : dps[-1], "maxDateTime" : "$$NOW"}}
        return pipe
    
    def generate_measurement_new(self, id_cnt):
        '''
            Generate updates to add to measurement array
        '''
        cur = datetime.now()
        device_id = random.randint(1000,9999)
        device_details = self.device_type(device_id)
        rlow = device_details["avg"] * 10
        rhigh = int(device_details["avg"] * 10 * 1.1)
        measurement_id = f"M-{id_cnt}"
        cur_val = device_details["avg"] + ((random.randint(1,100)/100) * device_details["avg"])
        doc = {
            "measurement_id" : measurement_id,
            "datetime" : cur,
            "cur_value" : cur_val,

        }
        return doc

    # TODO turn this on for the normal load
    #wait_time = between(1, 1)

    def device_type(self, device_id):
        # device_ids between 1000 and 10000
        brak = round(device_id, -3)
        characteristics = {
            1000 : {"type" : "chiller", "avg" : 55, "unit" : "degrees"},
            2000 : {"type" : "air_handler", "avg" : 230, "unit" : "cfm"},
            3000 : {"type" : "boiler", "avg" : 180, "unit" : "degrees"},
            4000 : {"type" : "vav", "avg" : 85, "unit" : "degrees"},
            5000 : {"type" : "air_handler", "avg" : 35, "unit" : "bars"},
            6000 : {"type" : "fan", "avg" : 2600, "unit" : "rpm"},
            7000 : {"type" : "room_temp", "avg" : 65, "unit" : "degrees"},
            8000 : {"type" : "set_point", "avg" : 90, "unit" : "percent"},
            9000 : {"type" : "power", "avg" : 40, "unit" : "amps"},
            10000 : {"type" : "distribution", "avg" : 400, "unit" : "amps"}
        }
        return characteristics[brak]

    def bulk_writer(self,collection, bulk_arr, msg = ""):
        try:
            result = collection.bulk_write(bulk_arr, ordered=False)
            ## result = db.test.bulk_write(bulkArr, ordered=False)
            # Opt for above if you want to proceed on all dictionaries to be updated, even though an error occured in between for one dict
            #pprint.pprint(result.bulk_api_result)
            note = f'BulkWrite - mod: {result.bulk_api_result["nModified"]} {msg}'
            #file_log(note,locker,hfile)
            print(note)
        except BulkWriteError as bwe:
            print("An exception occurred ::", bwe.details)

    def id_gen(self, icnt):
        # Get a bullk amount of ids
        idcoll = settings["db"][settings["count_coll"]]
        idcoll.find_and_modify({
            "query": { "_id": "UNIQUE COUNT DOCUMENT IDENTIFIER" },
            "update": {
                "$inc": {"counter": icnt },
            }
        })


    ################################################################
    # Since the loader is designed to be single threaded with 1 user
    # There's no need to set a weight to the task.
    # Do not create additional tasks in conjunction with the loader
    # If you are testing running queries while the loader is running
    # deploy 2 clusters in mLocust with one running faker and the
    # other running query tasks
    # The reason why we don't want to do both loads and queries is
    # because of the simultaneous users and wait time between
    # requests. The bulk inserts can take longer than 1s possibly
    # which will cause the workers to fall behind.
    ################################################################
    # TODO 0 this out if doing normal load
 
    @task(1)
    def _bulkinsert(self):
        # Note that you don't pass in self despite the signature above
        tic = self.get_time();
        name = "bulkinsert";

        global coll, auditcoll, audit, batch_size, settings

        try:
            update_arr = []
            base_size = int(batch_size * .01)
            # Now perform updates agains any value
            for _ in range(batch_size):
                upd_id = f"M-{random.randint(settings["base_id"],base_id + 5000000)}"
                res = self.generate_measurement(cur_id)
                update_arr.append(UpdateOne({"measurement_id": upd_id}, res))
                bulk_updates.append(res)
            bulk_writer(coll, update_arr)
            update_arr = []
        
            for _ in range(batch_size):
                arr.append(self.generate_result(cur_id))
                cur_id += 1
            #pprint.pprint(arr)
            
            #Note - swap for deployed - will crash mlocust
            #events.request_success.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0)
            events.request.fire(request_type="mlocust", name=name, response_time=(time.time()-tic)*1000, response_length=0)
            auditcoll.insert_one({"ts" : datetime.now(), "type" : "success", "action" : f"bulk_insert - {batch_size}", "msg" : f"response_time={(time.time()-tic)*1000}" })
            arr = []
        except Exception as e:
            #events.request_failure.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            events.request.fire(request_type="mlocust", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            self.audit("exception", e)
            # Add a sleep for just faker gen so we don't hammer the system with file not found ex
            #time.sleep(5)
