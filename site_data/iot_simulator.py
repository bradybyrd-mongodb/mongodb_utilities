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
    passed_args = {"args" : ARGS}
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
    if "demo" in args["args"]:
        batches = 100
    
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
    stub = {"$match": {"timestamp": {"$gt": datetime.datetime.fromisoformat("2024-06-05T00:12:32.164+00:00")}}}
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
        sub_start = datetime.datetime.now()
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
            elapsed = bb.timer(sub_start)
            bb.logit(f"[{pid}] - Batch Complete: {cur_batch} - size: {cnt}, Total:{tot}, Rate: {cnt/elapsed}")
            #print(f'Modified: {ids}')
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
    items["last_updated_at"] = ts
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

def update_identifiers():
    conn = client_connection()
    db = conn[settings["mongodb"]["database"]]
    ids = db.temp_idents.find({})
    aids = list(db.asset.find({}, {"_id": 1}))
    ctime = datetime.datetime.fromisoformat("2024-06-13T11:32:00.000+00:00")
    cnt = 0
    updates = []
    for it in ids:
        #conn.asset.update_one({"_id": aids[cnt]},{"$set": {"identifier": it["_id"], "updated_at" : ctime }})
        updates.append(UpdateOne({"_id": aids[cnt]["_id"]},{"$set": {"identifier": it["_id"], "updated_at" : ctime }}))
        print(f'Updating[{cnt}]: {aids[cnt]} -> {it["_id"]}')
        cnt += 1
    if updates:
        bulk_writer(db["asset"], updates)
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
    items["updated_at"] = ts
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

def update_asset_state(conn, last_updatetime):
    # Received the latest telemetry doc and updates values in current state
    items = {}
    cnt = 0
    assets = conn["asset"].find({"identifier": {"$exists" : True}},{"identifier": 1, "updated_at": 1}).limit(10)
    for device in assets:
        bb.logit(f'Updating[{cnt}]: {device["identifier"]}')
        pipeline = telemetry_pipeline(device["identifier"], last_updatetime)
        result = conn["asset"].aggregate(pipeline)
        recent_doc = {}
        for k in result:
            recent_doc = k
        #bb.logit("Recent doc for device:")
        #pprint.pprint(pipeline)
        #bb.logit("# --------------------------- #")
        if "merged" in recent_doc and recent_doc["merged"] != {}:
            #bb.logit("# ------------- OLD ------------- #")
            #pprint.pprint(recent_doc["current_state"])
            #bb.logit("# ------------- NEW ------------- #")
            #pprint.pprint(recent_doc["merged"])
            conn["asset"].update_one({"identifier": device["identifier"]}, {"$set": {"updated_at": recent_doc["results"]["updated_at"], "current_state": recent_doc["merged"]}})
        cnt += 1

def telemetry_pipeline(ident, start_time):
    # Find the fields reported for a device through time
    pipeline = [
        {"$match": {"identifier": ident}
        },
        {
            "$lookup": {
                "from": "telemetry",
                "localField": "identifier",
                "foreignField": "metadata.ident",
                "as": "results",
                "pipeline": [
                    {
                        "$match": {
                            "timestamp": {"$gt": start_time}
                        }
                    },
                    {
                        "$addFields": {
                            "measures.ts": "$timestamp"
                        }
                    },
                    {
                        "$project": {
                            "fieldNames": {"$objectToArray": "$measures"},
                            "ts": "$timestamp"
                        }
                    },
                    {
                        "$unwind": {"path": "$fieldNames"}
                    },
                    {
                        "$project": {
                            "key": "$fieldNames.k",
                            "value": "$fieldNames.v",
                            "ts": "$ts"
                        }
                    },
                    {
                        "$match": {"value": {"$ne": None}}
                    },
                    {
                        "$group": {
                            "_id": "$key",
                            "ex": {"$addToSet": "$value"},
                            "dataTypes": {"$addToSet": {"$type": "$value"}},
                            "count": {"$sum": 1},
                            "cur": {"$last": "$value"},
                            "ts": {"$last": "$ts"}
                        }
                    },
                    {
                    "$group": {
                        "_id": "",
                        "updated_at": {"$last": "$ts"},
                        "measures": {
                            "$addToSet": {
                                "k": "$_id",
                                "v": {
                                    "value": "$cur",
                                    "ts": "$ts"
                                }
                            }
                        }
                    }
                    },
                    {
                        "$project": {
                            "updated_at": 1,
                            "measures": {"$arrayToObject": "$measures"}
                        }
                    }
                ]
            }},
            {
                "$unwind": {"path": "$results"}
            },
            {
                "$addFields": {
                    "merged": {
                        "$mergeObjects": [
                            "$measures",
                            "$results.measures"
                        ]
                    }
                }
            }
        ]
    return pipeline

def update_asset_state_controller():
    # Update the current state of assets based on the latest telemetry data
    conn = client_connection()
    db = conn[settings["mongodb"]["database"]]
    last_updatetime = datetime.datetime.fromisoformat(settings["max_time"])
    bb.logit(f'Last update time: {last_updatetime}')
    update_asset_state(db, last_updatetime)
    conn.close()
    bb.logit("Asset state update complete.")

'''
Simulate a feed of IoT data into the telemetry collection
'''
def telemetry_feed():
    conn = client_connection()
    db = conn[settings["mongodb"]["feed_database"]]
    aids = list(db.asset.find({}, {"_id": 1}))
    feed_start = settings["feed_start"]
    feed_start_time = datetime.datetime.fromisoformat(feed_start)
    cnt = 0
    intervals = 10
    interval = 4  # hours
    #for k in range(intervals):
    start_time = datetime.datetime.fromisoformat(feed_start)
    end_time = start_time + datetime.timedelta(hours=interval)
    pipe = [
        {
            "$match": {
                "$and": [
                    {"timestamp": {"$gt": start_time}},
                    {"timestamp": {"$lt": end_time}}
                ]
            }
        },
        {"$sort": {"timestamp": 1}}
    ]
    feed = db.flespi.aggregate(pipe)
    cnt = 0
    for it in feed:
        print(f'Signal[{cnt}]: {it["timestamp"]} - {it["metadata"]["ident"]}')
        cnt += 1
        del(it["_id"])
        conn[settings["mongodb"]["database"]].telemetry_new.insert_one(it)
        time.sleep(0.1)
    conn.close()
    print("All Done")

def telmetry_update(update_doc):
    # Update the telemetry collection with the latest data
    conn = client_connection()
    db = conn[settings["mongodb"]["database"]]
    coll = "asset"
    ident = update_doc["ident"]
    ts = update_doc["updated_at"]
    measures = update_doc["measures"]
    db[coll].update_one(
        {"identifier": ident},
        [{"$set": {
            "current_state": {"$cond" : {
                "if": {"$gt": [ts, "$updated_at"]},
                "then": {"$mergeObjects": ["$current_state", measures]},
                "else": {"$mergeObjects": ["$current_state", {}]}
            }},
            "updated_at": {"$cond" : {
                "if": {"$gt": [ts, "$updated_at"]},
                "then": ts,
                "else": "$updated_at"
            }},
        }}]
    )
    conn.close()

def update_asset():
    udoc = {
        "ident": 'EVGU0000652',
        "updated_at": datetime.datetime.fromisoformat('2024-06-14T08:20:44.000Z'),
        "measures": {
            "container_DefrostInterval": { "value": 6, "ts": datetime.datetime.fromisoformat('2024-06-14T08:20:44.000Z') },
            "container_AmbientTemp": { "value": 26.4, "ts": datetime.datetime.fromisoformat('2024-06-14T08:20:44.000Z') },
            "container_Power": { "value": 19, "ts": datetime.datetime.fromisoformat('2024-06-14T08:20:44.000Z') },
        }
    }
    telmetry_update(udoc)
    print("All Done")

#----------------------------------------------------------------------#
#   Utility Routines
#----------------------------------------------------------------------#
def new_england_coordinates():
    lat_min = 40.477399  # Southernmost point (NYC)
    lat_max = 45.0152    # Northernmost point (Maine)
    lon_min = -79.7634   # Westernmost point (NY)
    lon_max = -66.9730   # Easternmost point (Maine)
    return [random.uniform(random.uniform(lon_min, lon_max), random.uniform(lat_min, lat_max))]

def update_asset_locations():
    """
    Update the location.coordinates field in the asset collection with random coordinates
    in the New York and New England area using Faker.
    """
    conn = client_connection()
    db = conn[settings["mongodb"]["database"]]
    assets = db["asset"]
    
    # New York and New England area bounds
    lat_min = 40.477399  # Southernmost point (NYC)
    lat_max = 45.0152    # Northernmost point (Maine)
    lon_min = -79.7634   # Westernmost point (NY)
    lon_max = -66.9730   # Easternmost point (Maine)
    
    # Get all assets that need updating
    assets_to_update = assets.find({}, {"_id": 1})
    updates = []
    for asset in assets_to_update:
        # Generate random coordinates within the specified bounds
        latitude = random.uniform(lat_min, lat_max)
        longitude = random.uniform(lon_min, lon_max)
        
        updates.append(UpdateOne(
            {"_id": asset["_id"]},
            {"$set": {
                "location": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                }
            }}
        ))
    
        if updates and len(updates) == 1000:
            try:
                result = assets.bulk_write(updates)
                bb.logit(f"Updated {result.modified_count} assets with new coordinates")
            except BulkWriteError as bwe:
                bb.logit(f"Error updating assets: {bwe.details}")
            updates = []
            bb.logit("Updated 1000 assets")
    if updates:
        try:
            result = assets.bulk_write(updates)
            bb.logit(f"Updated {result.modified_count} assets with new coordinates")
        except BulkWriteError as bwe:
            bb.logit(f"Error updating assets: {bwe.details}")
    conn.close()    

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

def resolve_device_fields(device="", start="", coll=None):
    # Find the fields reported for a device through time
    called = False
    if device != "":
        called = True
    elif "device" in ARGS:
        device = ARGS["device"]
    else:
        print("Device not specified")
        sys.exit(1)
    if start != "":
        ok = "ok"
    elif "start" in ARGS:
        start = ARGS["start"]
        # ISO: "2024-06-12T20:23:24.437+00:00"
    else:
        print("Start date not specified")
        sys.exit(1)
    if coll == None:
        conn = client_connection()
        db = conn[settings["mongodb"]["database"]]
        coll = db["telemetry"]
    pipe = [
        {
            "$match": {
                "timestamp": {"$gt": datetime.datetime.fromisoformat(start)},
                "metadata.ident": device
            }
        },
        {
            "$project": {
                "fieldNames": {"$objectToArray": "$measures"}
            }
        },
        {
            "$unwind": "$fieldNames"
        },
        {
            "$group": {
                "_id": "$fieldNames.k",
                "dataTypes": {"$addToSet": {"$type": "$fieldNames.v"}},
                "sampleValues": {"$addToSet": "$fieldNames.v"},
                "current": {"$last": {
                    "$cond": { 
                        "if" : {"$ne" : ["$fieldNmaes.v", None]},
                        "then": "$fieldNames.v",
                        "else": "$$REMOVE"
                    }
                }},
                "count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "fieldName": "$_id",
                "dataTypes": 1,
                "current": 1,
                "sampleValues": {"$slice": ["$sampleValues", 10]},
                "occurrenceCount": "$count",
                "_id": 0
            }
        },
        {
            "$sort": {"occurrenceCount": -1}
        }
    ]
    rec = coll.aggregate(pipe)
    print('# --------------------------- Device: {device} ------------------------------- #')
    for k in rec:
        #pprint.pprint(k)
        print(f'{k["fieldName"]}: {k["current"]} {k["dataTypes"]} cnt: {k["occurrenceCount"]}, ex: {k["sampleValues"]} ')   
    if not called:
        conn.close()

def idents_in_sample():
    # Find the fields reported for a device through time
    if "start" in ARGS:
        start = ARGS["start"]
        # ISO: "2024-06-12T20:23:24.437+00:00"
    else:
        print("Start date not specified")
        sys.exit(1)
    show_fields=False
    if "fields" in ARGS:
        show_fields = True
    conn = client_connection()
    db = conn[settings["mongodb"]["database"]]
    coll = db["telemetry"]
    pipe = [
    {
        "$match":{
            "timestamp": {"$gt": datetime.datetime.fromisoformat(start)}
        }
    },
    {
        "$group": {
        "_id": "$metadata.ident",
        "count": { "$sum": 1 }
        }
    },
    {"$sort": {"count": -1}}
    ]
    tot = 0
    cnt = 0
    rec = coll.aggregate(pipe)
    for k in rec:
        #pprint.pprint(k)
        print(f'{k["_id"]}: cnt: {k["count"]} ') 
        if show_fields and k["count"] > 10:
            resolve_device_fields(k["_id"], start, coll)
        tot += k["count"]
        cnt += 1
    print(f'Total ids: {cnt} docs: {tot}')
    conn.close()

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
    elif ARGS["action"] == "update_locations":
        update_asset_locations()
    elif ARGS["action"] == "update_dates":
        update_asset_update_date()
    elif ARGS["action"] == "device_fields":
        resolve_device_fields()
    elif ARGS["action"] == "telemetry_feed":
        telemetry_feed()
    elif ARGS["action"] == "telemetry_feed":
        telemetry_feed()
    elif ARGS["action"] == "update_asset":
        update_asset()
    elif ARGS["action"] == "update_state":
        update_asset_state_controller()
    elif ARGS["action"] == "unique_ids":
        idents_in_sample()
    elif ARGS["action"] == "test":
        test_mix()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()
