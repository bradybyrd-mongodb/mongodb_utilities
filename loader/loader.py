import sys
import os
import csv
from collections import OrderedDict
import json
import datetime
from decimal import Decimal
import random
import time
import re
import multiprocessing
import pprint
import pandas as pd
import bson
from bson.objectid import ObjectId
from bson.json_util import dumps
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from faker import Faker
import models as dgen
base_dir = os.path.dirname(os.path.abspath(__file__))
# apppend parent folder to path
sys.path.append(os.path.dirname(base_dir))
from bbutil import Util
from id_generator import Id_generator

fake = Faker()

'''
 #  Data loader Engine
 #  BJB 3/1/23
 
'''

def thread_loader():
    # read settings and echo back
    bb.message_box("Thread Loader", "title")
    bb.logit(f'# Settings from: {settings_file}')
    # Spawn processes
    num_procs = settings["process_count"]
    jobs = []
    inc = 0
    multiprocessing.set_start_method("fork", force=True)
    for item in range(num_procs):
        p = multiprocessing.Process(target=thread_worker, args = (item,params))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def thread_worker(ipos, passed_args):
    #  Working in parallel thread
    bb.message_box("Loading Synth Data")
    cur_process = multiprocessing.current_process()
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    #file_log(f'New process {cur_process.name}')
    start_time = datetime.datetime.now()
    conn = client_connection()
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = settings["batch_size"]
    bulk_docs = []
    if ipos == 0:
        # Start a primary thread process here
        it = "boo"
    else:
        for info in settings["models"]:
            substart_time = datetime.datetime.now()
            model = info["model"]
            if "count" in info:
                count = info["count"]
            collection = info["collection"]
            prefix = info["prefix"]
            IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefix})
            base_id = int(IDGEN.get(prefix, batch_size).replace(prefix,""))
            icnt = 0
            for counter in range(count):
                next_id = f'{prefix}-{base_id + counter}'
                info["id"] = next_id
                new_doc = dgen.generator(counter, info)
                bulk_docs.append(new_doc)
                if icnt == batch_size:
                    icnt = 0
                    bb.logit(f"Saving batch - {batch_size} - tot: {counter}")
                    db[collection].insert_many(bulk_docs)
                    bulk_docs = []
                icnt += 1
                # get the leftovers
            if len(bulk_docs) > 0:
                bb.logit("Saving last records")
                db[collection].insert_many(bulk_docs)
            execution_time = timer(substart_time)
            bb.logit(f"{cur_process.name} - {model} Load took {execution_time} seconds")

    execution_time = timer(start_time)
    #file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

#-----------------------------------------------------------------------#
#  Utility
#-----------------------------------------------------------------------#
def timer(t_start, quiet = True):
    #  Reads file and finds values
    end_time = datetime.datetime.now()
    time_diff = (end_time - t_start)
    execution_time = time_diff.total_seconds() + time_diff.microseconds * .000001
    if not quiet:
        cur_process = multiprocessing.current_process()
        procid = cur_process.name.replace("process", "p")
        print(f"{procid} - operation took {'{:.3f}'.format(execution_time)} seconds")
    return '{:.3f}'.format(execution_time)

def basic_test():
    info = settings["models"][0]
    info["id"] = "CIF-1000"
    result = dgen.generate(2,info)
    pprint.pprint(result)

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

def file_log(msg, locker = None, logger = None):
    cur_process = multiprocessing.current_process()
    procid = cur_process.name.replace("Process", "p")
    ctl_file = f"{procid}_run_log.txt"
    #if msg == "init":
    #    logger = open(ctl_file, "a")
    #    msg = "# -------------------------- Initializing Log ---------------------------#"
    cur_date = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    stamp = f"{cur_date}|I> "
    with open(ctl_file, 'a') as logger:
    #with locker:
        logger.write(f'{stamp}{msg}\n')
    return logger

#----------------------------------------------------------------------#
#   Utility Routines
#----------------------------------------------------------------------#

def client_connection(type = "uri", details = {}):
    global settings
    if not 'settings' in locals() and not 'settings' in globals():
        settings = details
    #pprint.pprint(settings)
    mdb_conn = settings[type]
    username = settings["username"]
    password = settings["password"]
    if not "@" in mdb_conn:
        if "username" in details:
            username = details["username"]
            password = details["password"]
        mdb_conn = mdb_conn.replace("//", f'//{username}:{password}@')
    #bb.logit(f'Connecting: {mdb_conn}')
    if "readPreference" in details:
        client = MongoClient(mdb_conn, readPreference=details["readPreference"]) #&w=majority
    else:
        client = MongoClient(mdb_conn)
    return client

#------------------------------------------------------------------#
#     MAIN
#------------------------------------------------------------------#
if __name__ == "__main__":
    global ARGS, settings
    settings_file = "loader_settings.json"
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    g_logger = file_log("init")
    base_counter = settings["base_counter"]
    IDGEN = Id_generator({"seed" : base_counter})
    MASTER_CUSTOMERS = []
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
        thread_loader()
    elif ARGS["action"] == "test":
        basic_test()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()

