#!/usr/bin/env python

'''
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!   NOTE: SCRIPT NEEDS TO BE COMPATIBLE WITH PYPY3!
!   THIS SAMPLE IS BUILT USING MIMESIS 11.1.0.
!   IF YOU ARE USING A SCRIPT THAT USES AN OLDER VERSION,
!   YOU NEED TO EITHER UPGRADE YOUR CODE TO MATCH THIS TEMPLATE
!   OR GO INTO THE REQUIREMENTS FILE AND CHANGE THE MIMESIS VERSION
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
'''

########################################################################
# 
# This is an example Locust file that use Mimesis to help generate
# dynamic documents. Mimesis is more performant than Faker
# and is the recommended solution. After you build out your tasks,
# you need to test your file in mLocust to confirm how many
# users each worker can support, e.g. confirm that the worker's CPU
# doesn't exceed 90%. Once you figure out the user/worker ratio,
# you should be able to figure out how many total workers you'll need
# to satisfy your performance requirements.
#
# These Mimesis locust files can be multi-use, 
# saturating a database with data or demonstrating standard workloads.
#
########################################################################

# Allows us to make many pymongo requests in parallel to overcome the single threaded problem
import gevent
from gevent import monkey
_ = gevent.monkey.patch_all()

########################################################################
# TODO Add any additional imports here.
# TODO But make sure to include in requirements.txt
########################################################################
import pymongo
from bson import json_util
from bson.json_util import loads
from bson import ObjectId
from bson.decimal128 import Decimal128
from locust import User, events, task, constant, tag, between, runners
import time
from pickle import TRUE
from datetime import datetime, timedelta
import random
from decimal import Decimal
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
from pymongo import UpdateMany
import string

# Global vars
# We can use this var to track the seq index of the worker in case we want to use it for generating unique seq keys in mimesis
_WORKER_ID = "T-1"
# Store the client conn globally so we don't create a conn pool for every user
# Track the srv globally so we know if we need to reinit the client
_CLIENT = None
_SRV = None
# Track the full host path so we know if anything changes
_HOST = None
_VERSION = None
#fake = Faker()

# docs to insert per batch insert
_SETTINGS = {
    "uri": "mongodb+srv://main_admin:bugsyBoo%21@iot-ingest.p3wh3.mongodb.net",
    "uri_check": "mongodb+srv://main_admin:bugsyBoo%21@claims-demo.vmwqj.mongodb.net",
    "database": "building_monitor",
    "collection": "readings",
    "base_id" : 1000000,
    "count_coll" : "counts",
    "batch_size": 1000,
    "username": "main_admin",
    "password": "<secret>",
    "version" : "3.1",
    "devices" : 40000,
    "pollers" : 10,
    "users" : 5,
    "gate" : "off",
    "device_types" : {
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
}
@events.init.add_listener
def on_locust_init(environment, **_kwargs):
    global _WORKER_ID
    if not isinstance(environment.runner, runners.MasterRunner):
        _WORKER_ID = environment.runner.worker_index

class MetricsLocust(User):
    ########################################################################
    # Class variables. 
    # The values are initialized with None
    # till they get set from the actual locust exeuction 
    # when the host param is passed in.
    # DO NOT HARDCODE VARS! PASS THEM IN VIA HOST PARAM.
    # TODO Do you have more than 20 tasks? If so, change the array init below.
    ########################################################################
    client, db, coll, bulk_size = None, None, None, None

    def __init__(self, parent):
        global _, _WORKER_ID, _HOST, _CLIENT, _SRV, _VERSION, _SETTINGS

        super().__init__(parent)

        try:
            database = _SETTINGS["database"] 
            collection = _SETTINGS["collection"]
            srv = _SETTINGS["uri"]
            batch_size = _SETTINGS["batch_size"]
            if _VERSION is None:
                _VERSION = _SETTINGS["version"]
            isInit = (_HOST != self.host)
            
            # Singleton
            if isInit:
                print("Initializing...")
                # Parse out env variables from the host
                vars = self.host.split("|")
                srv = vars[0]
                print("SRV:",srv)
                database = vars[1]
                collection = vars[2]
                # docs to insert per batch insert
                batch_size = int(vars[3])
                if len(vars) > 4:
                    _VERSION = vars[4]
                if len(vars) > 5:
                   _SETTINGS["pollers"] = int(vars[5])
                if len(vars) > 6:
                    _SETTINGS["users"] = int(vars[6])
                if len(vars) > 7:
                    _SETTINGS["gate"] = vars[7]
                self.client = pymongo.MongoClient(srv)
                _CLIENT = self.client
                _HOST = self.host           
            else:
                # standalone operation (testing)
                self.client = pymongo.MongoClient(srv)
                _HOST = srv
            _SRV = srv
            self.db = self.client[database]
            self.coll = self.db[collection]
            _SETTINGS["batch_size"] = batch_size
            # docs to insert per batch insert
            self.bulk_size = batch_size
            print("Batch size from Host:",self.bulk_size)
            srv_check = _SETTINGS["uri_check"]
            _SETTINGS["lclient"] = pymongo.MongoClient(srv_check)
        
            print("SRV:",srv)
            self.audit("init", f'Starting Params: C:{database}.{collection}, B:{batch_size}, V:{_VERSION}')

            # init schema once (mimesys here)
            if isInit:
                boo = "boo"
        except Exception as e:
            # If an exception is caught, Locust will show a task with the error msg in the UI for ease
            events.request.fire(request_type="Host Init Failure", name=str(e), response_time=0, response_length=0, exception=e)
            raise e
        ################################################################

    # Audit should only be intended for logging errors
    # Otherwise, it impacts the load on your cluster since it's
    # extra work that needs to be performed on your cluster
    ################################################################
    def audit(self, type, msg, xtra = {}):
        global _VERSION
        print("Audit: ", msg)
        payload = {"type":type, "ts":datetime.now(), "version": _VERSION, "msg":str(msg)}
        if xtra != {}:
            for k in xtra:
                payload[k] = xtra[k]
        self.db.audit.insert_one(payload)

    ################################################################
    # Example helper function that is not a Locust task.
    # All Locust tasks require the @task annotation
    ################################################################
    def get_time(self):
        return time.time()

    ################################################################
    # We need to simulate polling result
    # this method will create polling response
    # with mcnt data points in the array
    ################################################################
    def generate_result(self, id_cnt, mcnt = 720):
        '''
            Scenario - rooftop has 500 devices
            each device reports per minute
            devices have a 10% range of operation in differing absolute amount
        '''
        dps = []
        cur = datetime.now()
        start = cur - timedelta(hours=24)
        device_id = random.randint(1000,9999)
        measurement_id = f"M-{id_cnt}"
        device_details = _SETTINGS["device_types"][round(device_id, -3)]
        rlow = device_details["avg"] * 10
        rhigh = int(device_details["avg"] * 10 * 1.1)
        for k in range(mcnt):
            dps.append(random.randint(rlow,rhigh)/10)
        minval = min(dps)
        minpos = dps.index(minval)
        mintime = start + timedelta(minutes=minpos)
        maxval = max(dps)
        maxpos = dps.index(maxval)
        maxtime = start + timedelta(minutes=maxpos)
        doc = {
            "measurement_id": measurement_id,
            "type": device_details["type"],
            "unit": device_details["unit"],
            "deviceDataID": device_id,
            "date": datetime.now(),
            "dataPoints": dps,
            "pointCount": mcnt,
            "pointMax": maxpos,
            "pointMin": minpos,
            "pointOffset": random.randint(0,mcnt),
            "lastPoint": cur,
            "minValue": minval,
            "minDateTime": mintime,
            "maxValue": maxval,
            "maxDateTime": maxtime,
            "totalValue":  sum(dps),
            "totalPoints": mcnt,
            "lastPointValue": dps[mcnt - 1],
            "version": _VERSION
        }
        return doc

    ################################################################
    # We need to simulate polling update
    # this method will update
    #  
    ################################################################
    def generate_measurement(self, cur_id):
        '''
            Generate updates to add to measurement array
        '''
        dps = []
        cur = datetime.now()
        device_id = random.randint(1000,9999)
        device_details = _SETTINGS["device_types"][round(device_id, -3)]
        rlow = device_details["avg"] * 10
        rhigh = int(device_details["avg"] * 10 * 1.1)
        for k in range(5):
            dps.append(random.randint(rlow,rhigh)/10)
        pipe = {"$addToSet" : {"dataPoints" : {"$each" : dps}},"$inc" : {"pointCount" : 5}, "$set" : { "lastPoint" : dps[-1], "maxDateTime" : cur, "date" : cur}}
        return pipe
    
    def generate_measurement_insert(self, cur_id):
        '''
            Generate updates to add to measurement array
        '''
        global version
        cur = datetime.now()
        device_id = random.randint(1000,9999)
        device_details = self.device_type(device_id)
        rlow = device_details["avg"] * 10
        rhigh = int(device_details["avg"] * 10 * 1.1)
        cur_val = device_details["avg"] + ((random.randint(1,100)/100) * device_details["avg"])
        doc = {
            "measurement_id" : cur_id,
            "ts" : cur,
            "cur_value" : cur_val,
            "version" : version
        }
        return doc

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
        idcoll = self.db[_SETTINGS["count_coll"]]
        ans = idcoll.find_one_and_update({ "_id": "UNIQUE COUNT DOCUMENT IDENTIFIER" },
            {"$inc": {"counter": icnt }})
        return ans["counter"]
    
    def execution_gate(self):
        global _SETTINGS, _WORKER_ID
        if _SETTINGS["gate"] == "off":
            return True
        lclient = _SETTINGS["lclient"]
        ans = lclient.execution_gate.gates.find_one({"_id" : _WORKER_ID})
        if ans is None:
            lclient.execution_gate.gates.insert_one({"_id" : _WORKER_ID, "gate" : "off", "ts" : datetime.now()})
            return True
        elif ans["gate"] == "on" and ans["users"] > 3:
            lclient.execution_gate.gates.update_one({"_id" : _WORKER_ID},{"$set" : { "gate" : "off", "ts" : datetime.now(), "users" : 0}})
            return False
        elif ans["gate"] == "on":
            lclient.execution_gate.gates.update_one({"_id" : _WORKER_ID},{"$inc" : {"users" : 1}})
            return True
        else:
            return False

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
    @task(0)
    def _bulkinsert(self):
        global _SETTINGS 

        # Note that you don't pass in self despite the signature above
        tic = self.get_time()
        name = "bulkInsert"
        batch_size = self.bulk_size
        try:
            arr = []
            cur_id = self.id_gen(batch_size)
            
            for _ in range(batch_size):
                arr.append(self.generate_result(cur_id, 5))
                cur_id += 1
            
            self.db.insert_many(arr, ordered=False)
            #Note - swap for deployed - will crash mlocust
            #events.request_success.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0)
            events.request.fire(request_type="mlocust", name=name, response_time=(time.time()-tic)*1000, response_length=0)
            self.db.audit.insert_one({"ts" : datetime.now(), "type" : "success", "action" : f"bulk_insert - {batch_size}", "msg" : f"response_time={(time.time()-tic)*1000}", "version": version })
            arr = []
            events.request.fire(request_type="mlocust", name=name, response_time=(self.get_time()-tic)*1000, response_length=0)
        except Exception as e:
            events.request.fire(request_type="mlocust", name=name, response_time=(self.get_time()-tic)*1000, response_length=0, exception=e)
            # Add a sleep so we don't overload the system with exceptions
            time.sleep(5)

    @task(1)
    def _bulkupdate(self):
        # Note that you don't pass in self despite the signature above
        '''
            Ideally, do this every 30 seconds forever as a single execution
            so, if extra users represent the same poller, divide by that
        '''
        global _VERSION, _SETTINGS, _WORKER_ID
        # Force to execute every 30 seconds using external gate
        if False: #not self.execution_gate():
            #print("Bypassing gate")
            self.audit("info",f"Bypassing gate {_WORKER_ID}")
            return True
        poll_time = int(30/_SETTINGS["users"])
        tic = self.get_time()
        name = "bulkupdate"
        batch_size = self.bulk_size
        batches = int(_SETTINGS["devices"]/(batch_size * _SETTINGS["users"]))
        try:
            update_arr = []
            id_arr = []
            base_id = _SETTINGS["base_id"]
            cur_id = self.id_gen(0)
            # Now perform updates agains any value
            for it in range(batches):
                print(f'Performing batch {it}')
                subtic = self.get_time()
                for _ in range(batch_size):
                    upd_id = f"M-{random.randint(base_id, cur_id)}"
                    res = self.generate_measurement(upd_id)
                    update_arr.append(UpdateOne({"measurement_id": upd_id}, res))
                    id_arr.append(upd_id)
                self.bulk_writer(self.coll, update_arr)
                self.audit("tally", f"[{_WORKER_ID}] bulk_update - {batch_size}-{it} - response_time={'{:.3f}'.format((time.time()-subtic)*1000)}", {"tally" : batch_size} )
                update_arr = []
            #pprint.pprint(id_arr)
            events.request.fire(request_type="mlocust", name=name, response_time=(time.time()-tic)*1000, response_length=0)
            self.audit("success", f"[{_WORKER_ID}] bulk_update-done - {batch_size * batches} - response_time={'{:.3f}'.format((time.time()-tic)*1000)}" )
            interval = int(poll_time - (time.time() - tic))
            if interval > 0:
                time.sleep(interval)
        except Exception as e:
            #events.request_failure.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            events.request.fire(request_type="mlocust", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            self.audit("exception", e)
            # Add a sleep for just faker gen so we don't hammer the system with file not found ex
            #time.sleep(5)