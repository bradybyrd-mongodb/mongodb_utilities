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
client = None
coll = None
# Log all application exceptions (and audits) to the same cluster
audit = None

#fake = Faker()

# docs to insert per batch insert
batch_size = 1000
settings = {
    "uri": "mongodb+srv://claims-demo.vmwqj.mongodb.net",
    "database": "building_monitor",
    "collection": "readings",
    "username": "main_admin",
    "password": "<secret>",

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

        global client, coll, audit, batch_size, settings

        # Singleton
        if (client is None):
            # Parse out env variables from the host
            vars = self.host.split("|")
            srv = vars[0]
            print("SRV:",srv)
            client = pymongo.MongoClient(srv)

            db = client[vars[1]]
            coll = db[vars[2]]
            
            # Log all application exceptions (and audits) to the same cluster
            audit = client.mlocust.audit

            # docs to insert per batch insert
            batch_size = int(vars[3])
            print("Batch size from Host:",batch_size)

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
        audit.insert_one({"type":type, "ts":datetime.now(), "msg":str(msg)})

    ################################################################
    # We need to simulate polling result
    # this method will create polling response
    # with 720 data points in the array
    ################################################################
    def generate_results(self):
        '''
            Scenario - rooftop has 500 devices
            each device reports per minute
            devices have a 10% range of operation in differing absolute amount
        '''
        dps = []
        cur = datetime.now()
        start = cur - timedelta(hours=24)
        device_id = random.randint(1000,9999)
        device_details = self.device_type(device_id)
        rlow = device_details["avg"] * 10
        rhigh = int(device_details["avg"] * 10 * 1.1)
        for k in range(720):
            dps.append(random.randint(rlow,rhigh)/10)
        minval = min(dps)
        minpos = dps.index(minval)
        mintime = start + timedelta(minutes=minpos)
        maxval = max(dps)
        maxpos = dps.index(maxval)
        maxtime = start + timedelta(minutes=maxpos)
        doc = {
            "type": device_details["type"],
            "unit": device_details["unit"],
            "deviceDataID": device_id,
            "date": datetime.now(),
            "dataPoints": dps,
            "pointCount": 720,
            "pointMax": maxpos,
            "pointMin": minpos,
            "pointOffset": random.randint(0,720),
            "lastPoint": cur,
            "minValue": minval,
            "minDateTime": mintime,
            "maxValue": maxval,
            "maxDateTime": maxtime,
            "totalValue":  sum(dps),
            "totalPoints": 720,
            "lastPointValue": dps[719]
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

        global coll, audit

        try:
            arr = []
            for _ in range(batch_size):
                arr.append(self.generate_results())
            #pprint.pprint(arr)
            coll.insert_many(arr, ordered=False)

            #events.request_success.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0)
            events.request.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0)
        except Exception as e:
            #events.request_failure.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            events.request.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            self.audit("exception", e)
            # Add a sleep for just faker gen so we don't hammer the system with file not found ex
            #time.sleep(5)
