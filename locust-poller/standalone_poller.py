#!/usr/bin/env python

########################################################################
# Add any additional imports here.
# But make sure to include in requirements.txt
########################################################################
import os
import sys
from pymongo import MongoClient
from bson import json_util
from bson.json_util import loads
from bson import ObjectId
import multiprocessing
import urllib
import time
from pickle import TRUE
import datetime
import random
#from faker import Faker
import pprint
from bbutil import Util

# docs to insert per batch insert

settings = {
    "uri": "mongodb+srv://iot-ingest.p3wh3.mongodb.net",
    "database": "building_monitor",
    "collection": "readings",
    "username": "main_admin",
    "password": "<secret>",
    "batch_size" : 1000,
    "process_count" : 10,
    "batches" : 10,
    "id_start" : 3500000,
    "version" : "3.0"

}

def synth_data_load():
    # python3 rx_generator.py action=load_data
    multiprocessing.set_start_method("fork", force=True)
    bb.message_box("Loading Data", "title")
    num_procs = settings["process_count"]
    id_base = settings["id_start"]
    batch_size = settings["batch_size"]
    if "version" in ARGS:
        version = ARGS["version"]
        settings["version"] = version
    if "batch_size" in ARGS:
        batch_size = int(ARGS["batch_size"])
        settings["batch_size"] = batch_size
    batches = settings["batches"]
    if "batches" in ARGS:
        batches = int(ARGS["batches"])
        settings["batches"] = batches
    numtodo = batches * batch_size
    settings["numrecords"] = numtodo
    passed_args = {"settings" : settings, "args" : ARGS}
    bb.logit(f'# Loading: {num_procs * batches * batch_size} docs from {num_procs} threads')
    jobs = []
    inc = 0
    for item in range(num_procs):
        passed_args["cur_id"] = id_base
        p = multiprocessing.Process(target=worker_load, args = (item, passed_args))
        jobs.append(p)
        p.start()
        time.sleep(1)
        id_base += numtodo
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def worker_load(ipos, passed_args):
    #  Generate data in this thread
    cur_process = multiprocessing.current_process()
    pid = cur_process.name.replace("Process","P") #cur_process.pid
    bb = Util()
    settings = passed_args["settings"]
    conn = client_connection("uri", settings)
    tot_records = settings["numrecords"]
    batch_size = settings["batch_size"]
    version = settings["version"]
    batches = int(tot_records/batch_size) #settings["batches"]
    if batches == 0 : batches = 1
    cur_id = passed_args["cur_id"]
    settings["id_base"] = cur_id
    bb.message_box(f"[{pid}] Worker Data - generating {tot_records} recs", "title")
    bb.logit('Current process is %s %s' % (cur_process.name, pid))
    start_time = datetime.datetime.now()
    collection = settings["collection"]
    db = conn[settings["database"]]
    settings["db"] = db
    ts_start = datetime.datetime.now()
    cur_time = ts_start
    cnt = 0
    tot = 0
    # Loop through batches
    for k in range(batches):
        build_batch(settings, cur_id)
        bb.logit(f"[{pid}] - Loading batch: {k} - size: {batch_size}, Total:{tot}")
        cur_id += batch_size
        tot += batch_size
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    conn.close()
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def build_batch(settings, cur_id):
    # Note that you don't pass in self despite the signature above
    tic = get_time()
    name = "bulkinsert"
    batch_size = settings["batch_size"]
    version = settings["version"]
    db = settings["db"]
    coll = db[settings["collection"]]
    try:
        arr = []
        for _ in range(batch_size):
            arr.append(generate_results(cur_id, version))
            cur_id += 1
        #pprint.pprint(arr)
        coll.insert_many(arr, ordered=False)
        db.audit.insert_one({"ts" : datetime.datetime.now(), "type" : "success", "version" : version, "action" : f"bulk_insert - {batch_size}", "msg" : f"response_time={(time.time()-tic)*1000}" })
    except Exception as e:
        db.audit.insert_one({"ts" : datetime.datetime.now(), "type" : "failure", "version" : version, "action" : f"exception - {e}", "msg" : f"Promblems" })
        #time.sleep(5)


################################################################
# Example helper function that is not a Locust task.
# All Locust tasks require the @task annotation
# You have to pass the self reference for all helper functions
################################################################
def get_time():
    return time.time()

################################################################
# We need to simulate polling result
# this method will create polling response
# with 720 data points in the array
################################################################
def generate_results(id, version):
    '''
        Scenario - 80 pollers, each represents 20k devices
        each reports per minute for all devices
        devices have a 10% range of operation in differing absolute amount
    '''
    dps = []
    cur = datetime.datetime.now()
    start = cur - datetime.timedelta(hours=24)
    device_id = random.randint(1000,9999)
    device_details = device_type(device_id)
    rlow = device_details["avg"] * 10
    rhigh = int(device_details["avg"] * 10 * 1.1)
    for k in range(720):
        dps.append(random.randint(rlow,rhigh)/10)
    minval = min(dps)
    minpos = dps.index(minval)
    mintime = start + datetime.timedelta(minutes=minpos)
    maxval = max(dps)
    maxpos = dps.index(maxval)
    maxtime = start + datetime.timedelta(minutes=maxpos)
    doc = {
        "measure_id" : f'D-{id}',
        "type": device_details["type"],
        "unit": device_details["unit"],
        "deviceDataID": device_id,
        "date": datetime.datetime.now(),
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
        "lastPointValue": dps[719],
        "version": version
    }
    return doc

def device_type(device_id):
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




# --------------------------------------------------------- #
#       UTILITY METHODS
# --------------------------------------------------------- #
 
def timer(starttime,cnt = 1, ttype = "sub", prompt = "Operation took"):
    elapsed = datetime.datetime.now() - starttime
    secs = elapsed.seconds
    msecs = elapsed.microseconds
    if secs == 0:
        elapsed = msecs * .001
        unit = "ms"
    else:
        elapsed = secs + (msecs * .000001)
        unit = "s"
    if ttype == "sub":
        bb.logit(f"query ({cnt} recs) took: {'{:.3f}'.format(elapsed)} {unit}")
    elif ttype == "basic":
        bb.logit(f"{prompt}: {'{:.3f}'.format(elapsed)} {unit}")
    else:
        bb.logit(f"# --- Complete: query took: {'{:.3f}'.format(elapsed)} {unit} ---- #")
        bb.logit(f"#   {cnt} items {'{:.3f}'.format((elapsed)/cnt)} {unit} avg")

def client_connection(type="uri", details={}):
    if details != {}:
        settings = details
    mdb_conn = settings[type]
    username = settings["username"]
    password = settings["password"]
    if "username" in details:
        username = details["username"]
        password = details["password"]
    if "secret" in password:
        password = os.environ.get("_PWD_")
    password = urllib.parse.quote_plus(password)
    mdb_conn = mdb_conn.replace("//", f"//{username}:{password}@")
    bb.logit(f"Connecting: {mdb_conn}")
    if "readPreference" in details:
        client = MongoClient(
            mdb_conn, readPreference=details["readPreference"]
        )  # &w=majority
    else:
        client = MongoClient(mdb_conn)
    return client

# --------------------------------------------------------- #
#       MAIN
# --------------------------------------------------------- #
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "load_data":
        synth_data_load()
    else:
        print(f'{ARGS["action"]} not found')

    #conn.close()
