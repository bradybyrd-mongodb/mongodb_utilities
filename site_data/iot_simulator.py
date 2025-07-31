import sys
import os
from collections import defaultdict
from collections import OrderedDict
import json
import random
import time
import re
import multiprocessing
import pprint
import urllib
import copy
import uuid
from bson.objectid import ObjectId
from decimal import Decimal
from copy import deepcopy
base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(base_dir))
from bbutil import Util
#from id_generator import Id_generator
import process_csv_model as csvmod
import datetime
import pymongo
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
from pymongo import UpdateMany
from faker import Faker


bb = Util()
fake = Faker()
settings_file = "site_settings.json"

# Global settings
ARGS = {}  # To be populated from command line args
domain = "not-yet"

def synth_data_load():
    # python3 relational_replace_loader.py action=load_data
    multiprocessing.set_start_method("fork", force=True)
    bb.message_box("Loading Data", "title")
    bb.logit(f'# Settings from: {settings_file}')
    passed_args = {"ddl_action" : "info"}
    if "template" in ARGS:
        template = ARGS["template"]
        passed_args["template"] = template
    elif "data" in settings:
        goodtogo = True
    else:
        print("Send template=<pathToTemplate>")
        sys.exit(1)    # Spawn processes
    num_procs = settings["process_count"]
    batch_size = settings["batch_size"]
    batches = settings["batches"]
    bb.logit(f'# Loading: {num_procs * batches * batch_size} docs from {num_procs} threads')
    jobs = []
    inc = 0
    for item in range(num_procs):
        p = multiprocessing.Process(target=worker_load, args = (item, passed_args))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def worker_load(ipos, args):
    #  Called for each separate process
    cur_process = multiprocessing.current_process()
    pid = cur_process.pid
    conn = client_connection()
    bb.message_box(f"[{pid}] Worker Data", "title")
    batch_size = settings["batch_size"]
    batches = settings["batches"]
    coll = "flespi_base"
    dest_coll = "asset"
    # Set process-specific random seed
    random.seed(pid)       
    # Initialize counters and logging
    bb.logit('Current process is %s %s' % (cur_process.name, pid))
    start_time = datetime.datetime.now()
    db = conn[settings["mongodb"]["database"]]
    bulk_docs = []
    tot = 0
    pipe = [
        {"$sample": {"size" : batch_size}}
    ]
    count = int(batches * batch_size)
    base_counter = settings["base_counter"] + count * ipos + 1
    batches = int(count/batch_size)
    bb.message_box(f'[{pid}] base: {base_counter}, making: {count} in {batches} batches', "title")
    tot = 0
    if count < batch_size:
        batch_size = count
    print("# --------------------------------------------------------------- #")
    if batches == 0:
        batches = 1
    for cur_batch in range(batches):
        bb.logit(f"[{pid}] - Loading batch: {cur_batch}, {batch_size} records")
        cnt = 0
        results = db[coll].aggregate(pipe)
        bulk_docs, bulk_updates, ids = build_batch_from_template(results)
        cnt = len(bulk_docs)
        if cnt > 0:
            bb.logit(f"Generating IOT data, cnt: {cnt}")
            bulk_writer(db[dest_coll], bulk_updates)
            bulk_updates = []
            tot += cnt
            db[coll].insert_many(bulk_docs)
            bulk_docs = []
            bb.logit(f"[{pid}] - Batch Complete: {cur_batch} - size: {cnt}, Total:{tot}")
            print(f'Modified: {ids}')
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    conn.close()
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def build_batch_from_template(records):
    ids = []
    inserts = []
    bulk_updates = []
    for rec in records: # iterate through the telemetry
        # A dictionary that will provide consistent, random list lengths
        #pprint.pprint(rec)
        if rec["metadata"]["ident"] not in ids:
            ids.append(rec["metadata"]["ident"])
            new_rec = batch_build_doc(rec)
            inserts.append(new_rec)
            bulk_updates.append(batch_build_update(new_rec))
    return inserts, bulk_updates, ids

def apply_variation(value):
    """Apply random variation to a numeric value within the specified range."""
    variation = settings["variation"]
    if value == 0:
        return value
    variation_amount = random.uniform(-variation, variation)
    return value * (1 + variation_amount)

def batch_build_update(rec):
    """building the update for asset doc"""
    items = {}
    #pprint.pprint(telemetry_doc)
    measures = rec["measures"]
    ts = rec["timestamp"]
    for k in measures:
        if measures[k] is not None:
            items[f"current_state.{k}"] = {"value": measures[k], "updated" : ts}
    items["version_update"] = settings["version"]
    return UpdateOne({"identifier" : rec["metadata"]["ident"]},{"$set": items})

    
def batch_build_doc(rec):
    """Generate a variant of the IoT rec."""
    variant = deepcopy(rec)
    
    # Apply variation to all numeric, non-zero values in the measures section
    for key, value in variant['measures'].items():
        if isinstance(value, (int, float)) and value != 0:
            variant['measures'][key] = apply_variation(value)
    del variant["_id"]
    # Update the timestamp to reflect the variant creation
    variant_number = random.randint(60, 300)
    new_timestamp = datetime.datetime.fromisoformat(settings["max_time"]) #rec["timestamp"]
    new_timestamp += datetime.timedelta(seconds=variant_number)
    variant['timestamp'] = new_timestamp
    variant["version"] = settings["version"]
    return variant

def update_identifiers(conn, args):
    ids = conn.temp_idents.find({})
    id_range = [1000000,1005000]
    cnt = 0
    for it in ids:
        cur = f'A-{random.randint(id_range[0],id_range[1])}'
        conn.asset.update_one({"_id": cur},{"$set": {"identifier": it["_id"]}})
        print(f'Updating[{cnt}]: {cur}')
        cnt += 1
    print("All Done")

def update_current_state(conn, telemetry_doc):
    # Received the latest telemetry doc and updates values in current state
    items = {}
    #pprint.pprint(telemetry_doc)
    measures = telemetry_doc["measures"]
    ts = telemetry_doc["timestamp"]
    for k in measures:
        if measures[k] is not None:
            items[f"current_state.{k}"] = {"value": measures[k], "updated" : ts}
    #items.append({"timestamp": ts})
    conn["asset"].update_one({"identifier": telemetry_doc["metadata"]["ident"]}, {"$set": items})

def update_states(conn, args):
    rec_ids = ["SIM00000619"]
    if "ids" in args:
        rec_ids = args["ids"].split(",")
    cur_date_s = settings["max_time"]
    print("Updating current state")
    #result = conn["flespi_base"].find({"metadata.ident" : rec_ids, "timestamp" : {"$gt": datetime.datetime.strptime(cur_date_s, format_str)}})
    for it in rec_ids:
        print(f'Update: {it}')
        pipe = [
            {"$match":{"metadata.ident": it,"timestamp": { "$gt": datetime.datetime.fromisoformat(cur_date_s)}}},
            {"$sort":{"timestamp": -1}},
            {"$limit": 1}
        ]
        rec = conn["flespi_base"].aggregate(pipe)
        for k in rec:
            update_current_state(conn, k)

#----------------------------------------------------------------------#
#   Utility Routines
#----------------------------------------------------------------------#
def bulk_writer(collection, bulk_arr, msg = ""):
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

def client_connection(type = "uri", details = {}):
    lsettings = settings["mongodb"]
    mdb_conn = lsettings[type]
    username = lsettings["username"]
    password = lsettings["password"]
    if "secret" in password:
        password = os.environ.get("_PWD_")
    if "username" in details:
        username = details["username"]
        password = details["password"]
    if "%" not in password:
        password = urllib.parse.quote_plus(password)
    mdb_conn = mdb_conn.replace("//", f'//{username}:{password}@')
    bb.logit(f'Connecting: {mdb_conn}')
    if "readPreference" in details:
        client = MongoClient(mdb_conn, readPreference=details["readPreference"]) #&w=majority
    else:
        client = MongoClient(mdb_conn)
    return client

#------------------------------------------------------------------#
#     MAIN
#------------------------------------------------------------------#
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    base_counter = settings["base_counter"]
    #IDGEN = Id_generator({"seed" : base_counter})
    id_map = defaultdict(int)
    _details_ = {} #set global to avoid passing args - note lives in a single process
    if "wait" in ARGS:
        interval = int(ARGS["wait"])
        if interval > 10:
            bb.logit(f'Delay start, waiting: {interval} seconds')
            time.sleep(interval)
    #conn = client_connection()
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "load_iot_data":
        synth_data_load()
    elif ARGS["action"] == "test":
        test_mix()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()
