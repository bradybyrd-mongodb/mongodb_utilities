import sys
import os
import csv
#import vcf
from collections import OrderedDict
import json
import random
import time
import re
import multiprocessing
import pprint
import bson
from bson.objectid import ObjectId
from bbutil import Util
import vcf_queries as cc
#import pysam
import datetime
from pymongo import MongoClient
from faker import Faker

fake = Faker()

'''
  TimeSeries Notes
  11/4/21 BJB


'''
settings_file = "timeseries_settings.json"

class id_generator:
    def __init__(self, details = {}):
        self.tally = 100000
        if "seed" in details:
            self.tally = seed
    def set(self, seed):
        self.tally = seed
        return(self.tally)

    def get(self):
        prefix = random.choice(letters)
        prefix += random.choice(letters)
        result = f'{prefix}{self.tally}'
        self.tally += 1
        return result

def synth_data_load():
    # python3 relational_replace_loader.py action=emr_data
    multiprocessing.set_start_method("fork", force=True)
    bb.message_box("Loading Data", "title")
    bb.logit(f'# Settings from: {settings_file}')
    # Spawn processes
    num_procs = settings["process_count"]
    batch_size = settings["batch_size"]
    batches = settings["batches"]
    bb.logit(f'# Loading: {num_procs * batches * batch_size} docs from {num_procs} threads')
    jobs = []
    inc = 0
    for device in settings["metadata"]["devices"].keys():
        p = multiprocessing.Process(target=worker_emr_sample, args = (device, inc,))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def worker_emr_sample(device, ipos):
    #  Reads EMR sample file and finds values
    conn = client_connection()
    bb.message_box("Loading Synth Data", "title")
    settings = bb.read_json(settings_file)
    batch_size = settings["batch_size"]
    batches = settings["batches"]
    freq = settings["metadata"]["devices"][device]
    dilution_factor = settings["dilution_factor"]
    cur_process = multiprocessing.current_process()
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    file_log(f'New process {cur_process.name}')
    start_time = datetime.datetime.now()
    collection = settings["collection"]
    db = conn[settings["database"]]
    id_num = settings["base_counter"] + (ipos * batch_size * batches)
    bulk_docs = []
    ts_start = datetime.datetime.now()
    devices = create_devices(device)
    cur_time = ts_start
    dev_cnt = len(devices)
    cnt = 0
    tot = 0
    # To build the data, start at midnight and add the increment in seconds for each measurement
    for batch in range(batches):
        somethini = "ooo"
        # Challenge if devices > batchSize
        for inc in range(batch_size):
            year = random.randint(2017,2020)
            month = random.randint(1,12)
            day = random.randint(1,28)
            prop = random.randint(1,50)
            cur_time = cur_time + datetime.timedelta(0,2) # 2 second increment
            device_id = devices[dev_cnt]
            name = fake.name()
            ts = datetime.datetime.now()
            doc = OrderedDict()
            doc['_id'] = device_id
            doc['timestamp'] = ts
            doc['value'] = val
            doc['quality'] = "good"
            #pprint.pprint(doc)
            bulk_docs.append(doc)
            cnt += 1
            tot += 1
            id_num += 1
        db[collection].insert_many(bulk_docs)
        bulk_docs = []
        cnt = 0
        bb.logit(f"{cur_process.name} Inserting Bulk, Total:{tot}")
        file_log(f"{cur_process.name} Inserting Bulk, Total:{tot}")
        #oktogo = checkfile()
        #if not oktogo:
        #    bb.logit("Received stop signal")
        #    break
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    conn.close()
    file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def create_devices():
    # Create n different devices based on template
    cnt = 0
    collection = "devices"
    items = settings["metadata"]["devices"]
    iters = int(range(settings["num_devices"])/len(items))
    id_num = settings["base_counter"]
    bulk = []
    result = []
    bb.logit(f"Building resolved devices {iters} to do")
    for ipos in range(iters):
        device_id = f"{device}-{ipos}-{id_num}"
        doc = {"device_template" : device, "_id" : device_id}
        bulk.append(doc)
        result.append(device_id)
        id_num += 1
    db[collection].insert_many(bulk_docs)
    return result

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

def synth_data_update():
    # read settings and echo back
    #  python3 vcf_loader.py action=emr_data
    multiprocessing.set_start_method("fork", force=True)
    bb.message_box("Updating Data", "title")
    start_time = datetime.datetime.now()
    bb.logit(f'# Settings from: {settings_file}')
    # Spawn processes
    num_procs = settings["process_count"]
    jobs = []
    inc = 0
    for item in range(num_procs):
        p = multiprocessing.Process(target=worker_updater, args = (item,))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    bb.logit(f"{cur_process.name} - Execution total took {execution_time} seconds")

def worker_updater(ipos):
    #  Reads EMR sample file and finds values
    conn = client_connection()
    bb.message_box("Updating EMR Data", "title")
    batches = settings["batches"]
    cur_process = multiprocessing.current_process()
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    file_log(f'New process {cur_process.name}')
    start_time = datetime.datetime.now()
    collection = settings["collection"]
    db = conn[settings["database"]]
    num = len(cc.doctors)
    cnt = 0
    tot = 0
    for doctor in cc.doctors:
        somethini = "ooo"

        ids = list(db[collection].find({},{"_id" : 1}).skip(tot).limit(500))
        db[collection].update_many({"_id" : {"$in" : ids}}, {"referring_physician" : doctor})
        tot += 500
        bb.logit(f"{cur_process.name} Updating, Total:{tot}")

    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    conn.close()
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def load_query():
    # read settings and echo back
    bb.message_box("Performing 10000 queries in 7 processes", "title")
    num_procs = 7
    jobs = []
    inc = 0
    for item in range(num_procs):
        p = multiprocessing.Process(target=run_query)
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    for i in jobs:
        i.join()

def run_query():
    cur_process = multiprocessing.current_process()
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    bb.logit("Performing 5000 queries")
    conn = client_connection()
    db = conn[settings["database"]]
    num = len(cc.lexicon)
    cnt = 0
    for k in range(int(5000/num)):
        for term in cc.lexicon:
            start = datetime.datetime.now()
            output = db.emr.find({"disease" : {"$regex" : f'^{term}.*'}}).count()
            if cnt % 100 == 0:
                end = datetime.datetime.now()
                elapsed = end - start
                secs = (elapsed.seconds) + elapsed.microseconds * .000001
                bb.logit(f"{cur_process.name} - Query: Disease: {term} - Elapsed: {format(secs,'f')} recs: {output} - cnt: {cnt}")
            cnt += 1
            #time.sleep(.5)

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

#------------------------------------------------------------------#
#     MAIN
#------------------------------------------------------------------#
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    if "wait" in ARGS:
        interval = int(ARGS["wait"])
        if interval > 10:
            bb.logit(f'Delay start, waiting: {interval} seconds')
            time.sleep(interval)
    #conn = client_connection()
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "load_catalog":
        load_catalog()
    elif ARGS["action"] == "load_customers":
        load_customers()
    elif ARGS["action"] == "load_orders":
        load_orders()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()
