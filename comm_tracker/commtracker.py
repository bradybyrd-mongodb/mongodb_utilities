import sys
import os
import csv
from collections import OrderedDict
from collections import defaultdict
import json
import datetime
from decimal import Decimal
import random
import time
import re
import multiprocessing
import pprint
from deepmerge import Merger
import itertools
import shutil
import bson
from bson.objectid import ObjectId
from bson.json_util import dumps
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo import ReplaceOne
from pymongo.errors import BulkWriteError
from faker import Faker
base_dir = os.path.dirname(os.path.abspath(__file__))
# apppend parent folder to path
sys.path.append(os.path.dirname(base_dir))
sys.path.append(os.path.join(base_dir, "templates"))
from t_profile import ProfileFactory
from t_activity import ActivityFactory
from bbutil import Util
from id_generator import Id_generator

fake = Faker()
letters = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
providers = ["cigna","aetna","anthem","bscbsma","kaiser"]

'''
 #  Wellness IOT
#  BJB 8/1/22
Intermediary Cache from DTShare (cdc from DataGeneral to Warehouse in teradata)

    python3 wellness_iot.py action=load_csv

# Startup Env:
    Atlas M10BasicAgain

#  Use Case
    CVS gathers fitness and activity data from external providers (multiple vendors)
    Data arrives in batches, somtimes multiple updates to the same doc

#  Metrics
    50ms writes
    100ms reads
    1M users (5-6000 now)
    Typical user is 100 recs/day
    Goal - 200K users:
        20M messages/day
        10M Query API hits

#  Methodology
    Simulate the feed/batch from provider - simple 30attr document represents an update on MF
    Store raw document - 2M/hr
    Update "current" state of activity (add workout details) -> 25% of traffic
    Create new activities -> 75% of traffic
    create master profile documents -> 200K 
    create trigger and function to update
        
'''
settings_file = "wellness_iot_settings.json"

def load_activities():
    # read settings and echo back
    bb.message_box("Activity Loader", "title")
    bb.logit(f'# Settings from: {settings_file}')
    # Spawn processes
    num_procs = settings["process_count"]
    jobs = []
    inc = 0
    multiprocessing.set_start_method("fork", force=True)
    for item in range(num_procs):
        p = multiprocessing.Process(target=worker_load, args = (item,))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def worker_load(ipos):
    #  Reads file and finds values
    bb.message_box("Loading Synth Data")
    cur_process = multiprocessing.current_process()
    tester = False
    if "test" in ARGS:
        tester = True
    feed = 1
    if "feed" in ARGS:
        feed = int(ARGS["feed"])
    profile = False
    if "profile" in ARGS:
        profile = True
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    #file_log(f'New process {cur_process.name}')
    start_time = datetime.datetime.now()
    if profile:
        worker_profile_load(feed)
    else:
        worker_activity_load(feed)
    #worker_claim_load(pgcon,tables)
    #worker_rx_load(pgcon,tables)
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    #file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def worker_activity_load(num_iterations):
    #  Send a copy of the claim with one or two field changes
    #  Add an updateDate and sequencenumber, updateName = TCD-1
    cur_process = multiprocessing.current_process()
    collection = settings["collection"]
    profilecoll = settings["alt_collection"]
    prefix = "ACT"
    interval = 1
    conn = client_connection()
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = settings["batch_size"]
    num_records = settings["num_records"]
    sampler = settings["num_updates"]
    sample_size = int(num_records * sampler)
    if "size" in ARGS:
        sample_size = int(ARGS["size"])
    pipe = [{"$sample" : {"size" : sample_size}}] # Do 10% at a time
    for k in range(num_iterations):
        bb.message_box("Activity Feed Simulation")
        bb.logit(f'Iter: {k} of {num_iterations}, {sample_size} per batch')
        profile_cur = db[profilecoll].aggregate(pipe)
        IDGEN.set({"seed" : base_counter, "size" : num_records, "prefix" : prefix})
        base_id = int(IDGEN.get(prefix, sample_size).replace(prefix,""))
        bulk_docs = []
        icnt = 0
        for row in profile_cur:
            new_id = f'{prefix}{base_id + icnt}'
            if icnt != 0 and icnt % batch_size == 0:
                db[collection].insert_many(bulk_docs)
                bb.logit(f'Adding {batch_size} total: {icnt}')
                bulk_docs = []
                time.sleep(interval)
            metrics = act.build_doc(new_id, row)
            bulk_docs.append(metrics)
            icnt += 1
        # get the leftovers
        bb.logit(bulk_docs)
        db[collection].insert_many(bulk_docs)
    bb.logit("#-------- COMPLETE -------------#")
    conn.close()

def worker_activity_update_new(num_iterations):
    #  Send a copy of the claim with one or two field changes
    #  Add an updateDate and sequencenumber, updateName = TCD-1
    cur_process = multiprocessing.current_process()
    collection = settings["collection"]
    profilecoll = settings["profile_collection"]
    rawcoll = settings["history_collection"]
    prefix = "ACT"
    feed = False
    act = ActivityFactory()
    interval = 0
    conn = client_connection()
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = settings["batch_size"]
    num_records = settings["num_records"]
    sampler = settings["num_updates"]
    act_template = settings["activity_template"]
    sample_size = int(num_records * sampler)
    if "repeat" in ARGS:
        num_iterations = int(ARGS["repeat"])
     
    if "feed" in ARGS:
        sample_size = 10
        num_iterations = int(ARGS["feed"])
        interval = 5
        feed = True
    if "size" in ARGS:
        sample_size = int(ARGS["size"])
    pipe = [{"$sample" : {"size" : sample_size}}] # Do 10% at a time
    for k in range(num_iterations):
        bb.message_box("Activity Feed Simulation")
        bb.logit(f'Iter: {k} of {num_iterations}, {sample_size} per batch')
        act_cur = db[profilecoll].aggregate(pipe)
        IDGEN.set({"seed" : base_counter, "size" : num_records, "prefix" : prefix})
        base_id = int(IDGEN.get(prefix, sample_size).replace(prefix,""))
        bulk_docs = []
        bulk_updates = []
        icnt = 0
        for row in act_cur:
            new_id = f'{prefix}{base_id + icnt}'
            cur_doc = bb.read_json(act_template)
            if icnt != 0 and icnt % batch_size == 0:
                if feed:
                    db[rawcoll].insert_many(bulk_docs)
                else:
                    bulk_writer(db[collection],bulk_updates)
                bb.logit(f'Adding {batch_size} total: {icnt}')
                bulk_updates = []
            metrics = act.build_doc(new_id, row, cur_doc)
            if feed:
                bulk_docs.append(metrics)
            else:
                for dat in metrics["data"]:
                    bulk_updates.append(ReplaceOne({"id" : dat["id"]},dat, upsert=True))
            icnt += 1
        # get the leftovers
        #bb.logit(f'Leftovers {bulk_docs}')
        if feed:
            db[rawcoll].insert_many(bulk_docs)
            if interval > 0:
                bb.logit(f'Delay, waiting: {interval} seconds')
                time.sleep(interval)
        else:
            bulk_writer(db[collection],bulk_updates)

    bb.logit("#-------- COMPLETE -------------#")
    conn.close()

def worker_activity_update(num_iterations):
    #  OLD one - now superseeded
    #  Add an updateDate and sequencenumber, updateName = TCD-1
    cur_process = multiprocessing.current_process()
    collection = settings["collection"]
    profilecoll = settings["alt_collection"]
    prefix = "ACT"
    act = ActivityFactory()
    interval = 1
    conn = client_connection()
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = settings["batch_size"]
    num_records = settings["num_records"]
    sampler = settings["num_updates"]
    sample_size = int(num_records * sampler)
    if "size" in ARGS:
        sample_size = int(ARGS["size"])
    pipe = [{"$sample" : {"size" : sample_size}}] # Do 10% at a time
    for k in range(num_iterations):
        bb.message_box("Activity Feed Simulation")
        bb.logit(f'Iter: {k} of {num_iterations}, {sample_size} per batch')
        act_cur = db[collection].aggregate(pipe)
        IDGEN.set({"seed" : base_counter, "size" : num_records, "prefix" : prefix})
        base_id = int(IDGEN.get(prefix, sample_size).replace(prefix,""))
        bulk_docs = []
        icnt = 0
        for row in act_cur:
            new_id = f'{prefix}{base_id + icnt}'
            if icnt != 0 and icnt % batch_size == 0:
                bulk_writer(db[collection],bulk_updates)
                bb.logit(f'Adding {batch_size} total: {icnt}')
                bulk_updates = []
                #bb.logit(f'Delay, waiting: {interval} seconds')
                time.sleep(interval)
            metrics = act.build_doc(new_id, row, row["id"])
            bulk_updates.append(UpdateOne({"_id" : row["_id"]},{"$addToSet" : {"data" : metrics}}))
            icnt += 1
        # get the leftovers
        bulk_writer(db[profilecoll],bulk_updates)
    bb.logit("#-------- COMPLETE -------------#")
    conn.close()

def worker_profile_load(num_iterations):
    #  Send a copy of the claim with one or two field changes
    #  Add an updateDate and sequencenumber, updateName = TCD-1
    cur_process = multiprocessing.current_process()
    collection = settings["profile_collection"]
    profilecoll = settings["collection"]
    prefix = "PROF"
    prof = ProfileFactory()
    interval = 1
    conn = client_connection()
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = settings["batch_size"]
    num_records = settings["num_records"]
    sampler = settings["num_updates"]
    sample_size = int(num_records * sampler)
    if "size" in ARGS:
        sample_size = int(ARGS["size"])
    for k in range(num_iterations):
        bb.message_box("Activity Feed Simulation - Profiles")
        bb.logit(f'Iter: {k} of {num_iterations}, {sample_size} per batch')
        IDGEN.set({"seed" : base_counter, "size" : num_records, "prefix" : prefix})
        base_id = int(IDGEN.get(prefix, sample_size).replace(prefix,""))
        bulk_docs = []
        bulk_updates = []
        icnt = 0
        for k in range(num_records):
            new_id = f'{prefix}{base_id + icnt}'
            if icnt != 0 and icnt % batch_size == 0:
                db[collection].insert_many(bulk_docs)
                bb.logit(f'Adding {batch_size} total: {icnt}')
                bulk_docs = []
                #bb.logit(f'Delay, waiting: {interval} seconds')
                time.sleep(interval)
            bulk_docs.append(prof.build_doc(new_id))
            icnt += 1
        # get the leftovers
        db[collection].insert_many(bulk_docs)
    bb.logit("#-------- COMPLETE -------------#")
    conn.close()

def worker_history_tester(num_iterations):
    #  Send a copy of the claim with one or two field changes
    #  Add an updateDate and sequencenumber, updateName = TCD-1
    collection = settings["alt_collection"]
    claimcoll = settings["collection"]
    prefix = "CLU"
    conn = client_connection()
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = settings["batch_size"]
    num_records = settings["num_records"]
    sampler = settings["num_updates"]
    sample_size = 50
    pipe = [{"$sample" : {"size" : sample_size}}] # Do 10% at a time
    claim_cur = db[claimcoll].aggregate(pipe)
    IDGEN.set({"seed" : base_counter, "size" : num_records, "prefix" : prefix})
    bb.logit("Getting id")
    base_id = int(IDGEN.get(prefix, sample_size).replace(prefix,""))
    bulk_docs = []
    icnt = 0
    bb.logit("Doing Loop")
    for row in claim_cur:
        new_id = f'{prefix}{base_id + icnt}'
        if icnt < num_iterations:
            bb.logit("Adding History: " + new_id)
            db[collection].insert_one(claim_doc(new_id, row))
        icnt += 1
    bb.logit("#-------- COMPLETE -------------#")
    conn.close()

def get_claim_ids(coll):
    MASTER_CLAIMS = []
    cur = coll.find({},{"_id" : 0 ,"claim_id" : 1})
    for it in cur:
        MASTER_CLAIMS.append(it["claim_id"])

#----------------------------------------------------------------------#
#   Reporting
#----------------------------------------------------------------------#
def claims_reports():
    #  Take an array of claims, give the timeline of changes
    #  Add an updateDate and sequencenumber, updateName = TCD-1
    cur_process = multiprocessing.current_process()
    report_type = "none"
    if "report" in ARGS:
        report_type = ARGS["report"]
    else:
        print("Send report=<report_type>")
        sys.exit(1)    # Spawn processes
    if report_type == "claim_history":
        history_report()
    elif report_type == "claim_details":
        claim_detail()
    else:
        print(f"No report called {report_type} choices: claim_history, claim_details")

#----------------------------------------------------------------------#
#   Utility Routines
#----------------------------------------------------------------------#

def client_connection(type = "uri", details = {}):
    mdb_conn = settings[type]
    username = settings["username"]
    password = settings["password"]
    if "username" in details:
        username = details["username"]
        password = details["password"]
    mdb_conn = mdb_conn.replace("//", f'//{username}:{password}@')
    bb.logit(f'Connecting: {mdb_conn}')
    if "readPreference" in details:
        client = MongoClient(mdb_conn, readPreference=details["readPreference"]) #&w=majority
    else:
        client = MongoClient(mdb_conn)
    return client

def increment_version(old_ver):
    parts = old_ver.split(".")
    return(f'{parts[0]}.{int(parts[1]) + 1}')

def check_file(type = "delete"):
    #  file loader.ctl
    ctl_file = "loader.ctl"
    result = True
    with open(ctl_file, 'w', newline='') as controlfile:
        status = controlfile.read()
        if "stop" in status:
            result = False
    return(result)

def file_log(msg):
    if not "file" in ARGS:
        return("goody")
    ctl_file = "run_log.txt"
    cur_date = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    stamp = f"{cur_date}|I> "
    with open(ctl_file, 'a') as lgr:
        lgr.write(f'{stamp}{msg}\n')

def bulk_writer(collection, bulk_arr):
    try:
        result = collection.bulk_write(bulk_arr)
        ## result = db.test.bulk_write(bulkArr, ordered=False)
        # Opt for above if you want to proceed on all dictionaries to be updated, even though an error occured in between for one dict
        pprint.pprint(result.bulk_api_result)
    except BulkWriteError as bwe:
        print("An exception occurred ::", bwe.details)


#------------------------------------------------------------------#
#     MAIN
#------------------------------------------------------------------#
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    CUR_PATH = os.path.dirname(os.path.realpath(__file__))
    base_counter = settings["base_counter"]
    IDGEN = Id_generator({"seed" : base_counter})
    
    MASTER_CLAIMS = []
    if "wait" in ARGS:
        interval = int(ARGS["wait"])
        if interval > 10:
            bb.logit(f'Delay start, waiting: {interval} seconds')
            time.sleep(interval)
    #conn = client_connection()
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "load_data":
        load_activities()
    elif ARGS["action"] == "load_updates":
        worker_activity_update_new(1)
    elif ARGS["action"] == "reports":
        claims_reports()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()

'''
#---- Data Load ---------------------#
python3 claimcache.py action=customer_load
python3 claimcache.py action=recommendations_load
python3 claimcache_pbm.py action=load_claim_updates test=true size=10

{"user.user_id" : {$in: ["PROF1000083","PROF1000107","PROF1000123","PROF1000244","PROF1000255","PROF1000702"]}}

'''