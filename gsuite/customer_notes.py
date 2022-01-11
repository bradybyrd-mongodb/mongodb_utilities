#------------------------------------------------#
#  File Walker - create db of all files in subdir
#------------------------------------------------#

import sys
import os
import csv
import json
import datetime
import random
import time
import re
import multiprocessing
import pprint
import bson
from bson.objectid import ObjectId
from ../bbutil import Util
from datetime import datetime
from collections import OrderedDict
from pymongo import MongoClient

'''
#------------------------------------------#
# Notes
#
'''

settings_file = "customer_notes_settings.json"
path ="C:/workspace/python"
jobs = []

def file_walk():
    #we shall store all the file names in this list
    collection = settings["collection"]
    bulk_docs = []
    testy = []
    path_list = []
    batch_size = settings["batch_size"]
    if "path" in ARGS:
        path = ARGS["path"]
    else:
        print("Send path= argument")
        sys.exit(1)
    main_process = multiprocessing.current_process()
    bb.logit("#------------------------------------------------------------#")
    bb.logit('# Main process is %s %s' % (main_process.name, main_process.pid))
    conn = client_connection()
    db = conn[settings["database"]]
    cnt = 0
    tot = 0

    for root, dirs, files in os.walk(path):
        bulk_docs = []
        cnt = 0
        bb.logit("#--------------------------------------------------#")
        bb.logit(f'# {root}')
        dir_doc = file_info(root, "dir")
        db[collection].insert_one(dir_doc)
        for fil in files:
            cur_file = os.path.join(root,fil)
            new_doc = (file_info(cur_file))
            bulk_docs.append(new_doc)
            bb.logit(cur_file)
            cnt += 1
            if cnt == batch_size:
                tot += batch_size
                bb.logit(f'Loading {batch_size} more - total: {tot}')
                db[collection].insert_many(bulk_docs)
                update_directories(bulk_docs, root)
                bulk_docs = []
                cnt = 0
        # Get leftovers
        if len(bulk_docs) > 0:
            tot += len(bulk_docs)
            bb.logit(f'Loading {len(bulk_docs)} more - total: {tot}')
            db[collection].insert_many(bulk_docs)
            update_directories(bulk_docs, root)

    for i in jobs:
        i.join()
    bb.logit(f'Completed {tot} directory items')
    conn.close()

def file_info(file_obj, type = "file"):
    try:
        file_stats = os.stat(file_obj)
        doc = OrderedDict()
        doc["path_raw"] = file_obj
        if type == "file":
            doc["is_object"] = True
            doc["num_objects"] = 0
            doc["size_kb"] = file_stats.st_size * .001
        else:
            doc["is_object"] = False
            doc["num_objects"] = 0
            doc["size_kb"] = 0
        doc["permissions"] = file_stats.st_mode
        doc["owner"] = f'{file_stats.st_uid}:{file_stats.st_gid}'
        m_at = file_stats.st_mtime
        c_at = file_stats.st_ctime
        doc["modified_at"] = datetime.fromtimestamp(m_at).strftime('%Y-%m-%d %H:%M:%S')
        doc["created_at"] = datetime.fromtimestamp(c_at).strftime('%Y-%m-%d %H:%M:%S')
        doc["paths"] = file_obj.split("/")
    except:
        bb.logit(f'Path: {file_obj} inaccessible')
        doc = {"error" : True}
    return(doc)

def load_paths():
    seed1 = settings["path_seeds"]["first"]
    seed2 = settings["path_seeds"]["second"]

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
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "walk":
        file_walk()
    else:
        print(f'{ARGS["action"]} not found')
