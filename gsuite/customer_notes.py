#------------------------------------------------#
#  File Walker - create db of all files in subdir
#------------------------------------------------#

import sys
import os
import inspect
import csv
import copy
import json
import datetime
import random
import time
import re
import multiprocessing
import pprint
import bson
from bson.objectid import ObjectId
from collections import OrderedDict
from pymongo import MongoClient

# Hack to import from parent directory
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from bbutil import Util

'''
#------------------------------------------#
# Notes
Read through all dirs in Customers
Exclude .files
By Folder:
    list all files
    note and ignore subfolder contents
    _notes.md file, import it
    any other .md file, import it

Notes File:
    #------------------------------------------#  <= Border
    #  Call 10/14/21 - Charts Help   <= Date, subject
    Data Science team <= People


Document Schema:
{
    customer: "Agero",
    folder: "Agero",
    rep: "Vin D'Amato",
    status: "active",
    "brief": "happy peppy customer",
    quality: "good",
    contacts: [
        {name: "Bob White", "position" : "Ent Architect", "notes" : "nice guy"}
    ],
    "notes" : [
        {"date" : "3-21-2021", "subject": "meeting", "body" : "lots of text"}
    ],
    "files" : [
        {"name" : "stuff.json", "path" : "fullpath", created: , "modified" , "size", is_object: True}
    ]
}
#
'''

settings_file = "customer_notes_settings.json"
path ="Users/"
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
        path = settings["base_document"]["path"]
        if not path:
            print("Send path= argument")
            sys.exit(1)
    main_process = multiprocessing.current_process()
    bb.logit("#------------------------------------------------------------#")
    bb.logit('# Main process is %s %s' % (main_process.name, main_process.pid))
    conn = client_connection()
    db = conn[settings["database"]]
    doc = OrderedDict()
    cnt = 0
    tot = 0
    company = "base_folder"
    last_path = path
    base_parts = path.split("/")
    base_depth = len(base_parts)
    for root, dirs, files in os.walk(path):
        bulk_docs = []
        file_arr = []
        cnt = 0
        cur_parts = root.split("/")
        if ".git" in root:
            continue
        if len(cur_parts) > (base_depth + 1):
            # deeper sub, ignore
            file_arr.append(file_info(root,"dir"))
            bb.logit(f'# {root} - subdir')
            continue
        if root != last_path:
            bulk_docs.append(doc)
            doc = OrderedDict()
            company = cur_parts[-1]
            last_path = root
            bb.logit("#--------------------------------------------------#")
            bb.logit(f'# Firm: {company}')
            bb.logit(f'# {root}')
            doc["customer"] = company
        else:
            doc["customer"] = company
        doc["path"] = root
        dir_doc = file_info(root, "dir")
        db[collection].insert_one(dir_doc)
        for fil in files:
            if fil[0] == ".":
                continue
            cur_file = os.path.join(root,fil)
            new_doc = (file_info(cur_file))
            file_arr.append(new_doc)
            if "_notes.md" in fil:
                process_notes(fil, root)
            bb.logit(cur_file)
            cnt += 1
            if cnt == batch_size:
                tot += batch_size
                bb.logit(f'Loading {batch_size} more - total: {tot}')
                db[collection].insert_many(bulk_docs)
                #update_directories(bulk_docs, root)
                bulk_docs = []
                cnt = 0
        # Get leftovers
        if len(bulk_docs) > 0:
            tot += len(bulk_docs)
            bb.logit(f'Loading {len(bulk_docs)} more - total: {tot}')
            db[collection].insert_many(bulk_docs)
            #update_directories(bulk_docs, root)

    for i in jobs:
        i.join()
    bb.logit(f'Completed {tot} directory items')
    conn.close()

def process_notes(name,fullpath):
    # Break up notes file
    content = ""
    with open(os.path.join(fullpath,name)) as f:
        content = f.readlines()
    notes = []
    bb.logit(f'Parsing Notes: {name}')
    note = ""
    event_date = "unknown" #datetime.datetime.now()
    subject = "Generic Meeting"
    body = ""
    in_header = -1
    cnt = 0
    for line in content:
        if "#---------------------------------" in line:
            bb.logit(f'header[{cnt}] - {line}')
            in_header = 1
            note = {"date" : event_date, "subject": subject, "body" : body}
            bb.logit(f'Adding: {note["date"]} - {note["subject"]}')
            notes.append(copy.deepcopy(note))
            event_date = "unknown" #datetime.datetime.now()
            subject = "Generic Meeting"
            body = ""
        else:
            if in_header == 1:
                if line.startswith("# "):
                    subject = line
                else:
                    in_header = -1
                res = line_has_date(line)
                if res != "none":
                    event_date = res
                bb.logit(f'subject[{cnt}] {event_date} - {line.strip()}')
                in_header += 1
            elif in_header == 2:
                if line.startswith("# "):
                    xtra = line
                    subject = f'{subject.strip()} | {line.strip()}'
                    in_header += 1
                else:
                    body = f'{body}{line}'
                    in_header = -1
                res = line_has_date(line)
                if res != "none":
                    event_date = res
                bb.logit(f'xtra[{cnt}] {event_date} - {line.strip()}')
            elif in_header == 3:
                if line.startswith("# "):
                    xtra = line
                    subject = f'{subject.strip()} | {line.strip()}'
                in_header = -1
                res = line_has_date(line)
                if res != "none":
                    event_date = res
                bb.logit(f'xtra[{cnt}] {event_date} - {line.strip()}')
            else:
                body += line
                bb.logit(f'body[{cnt}]')
        cnt += 1
    bb.logit("---- Processed ----")
    for k in notes:
        print("#---------------------------#")
        print(k["date"])
        print(f'Subject: {k["subject"]}')
        print(f'Body: \n{body}')
        print(" ")


def line_has_date(txt):
    #fin date string in line
    ans = re.findall(r"\d+\/\d+\/\d+",txt)
    month = 1
    day = 1
    year = 2010
    answer = "none"
    try:
        if len(ans) > 0:
            dat = ans[0]
            parts = dat.split("/")
            month = int(parts[0])
            day = int(parts[1])
            year = int(parts[2])
            if year < 100:
                year += 2000
            answer = datetime.datetime(year,month,day, 11, 59)
    except Exception as e:
        bb.logit(f'Error: {str(e)}, orig: {txt}')
    return(answer)

def file_info(file_obj, type = "file"):
    try:
        file_stats = os.stat(file_obj)
        doc = OrderedDict()
        paths = file_obj.split("/")
        doc["name"] = paths[-1]
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
        doc["modified_at"] = datetime.datetime.fromtimestamp(m_at).strftime('%Y-%m-%d %H:%M:%S')
        doc["created_at"] = datetime.datetime.fromtimestamp(c_at).strftime('%Y-%m-%d %H:%M:%S')
        doc["paths"] = paths
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
    elif ARGS["action"] == "process_notes":
        path = ARGS["path"]
        fil = ARGS["filename"]
        process_notes(fil, path)
    else:
        print(f'{ARGS["action"]} not found')
