from google.cloud import bigquery 
#from flask import Response
import os
import time
import datetime
import multiprocessing
import pprint
import string
import bson
from bson.objectid import ObjectId
from bson.json_util import dumps
from bbutil import Util
from id_generator import Id_generator
from pymongo import MongoClient

def get_settings():
    return {
        "process_count" : 3,
        "batch_size" : 10,
        "base_counter" : 1000,
        "batches" : 2
    }
#GCP Cloud Function!
def claim_polling_trigger(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Runs every 5 seeconds - triggerd buu scheduler
    Calls big query
    loop through results and send to confluent
    """
    client = bigquery.Client()
    print("Logging request: ")
    '''request_json = request.get_json()
    print(request_json)
    #  Reject if token doesnt match
    if "token" in request_json:
        if request_json["token"] == "sdfjlhag;JH98Hiudhf65HiNug!!":
            print("Token match - success")
        else:
            return Response({'message': 'Authorization token failed'}, status=403, mimetype='application/json')
    else:
        return Response({'message': 'Authorization token failed'}, status=403, mimetype='application/json')
    '''
    last_check = '2023-05-16 14:30:21'
    warehouse_db = "bradybyrd-poc.claims_warehouse.Provider"
    mdb_cred = "mongodb+srv://main_admin:<secret>@claims-demo.vmwqj.mongodb.net"
    mdbconn = MongoClient(mdb_cred)
    db = mdbconn["claim_demo"]
    ans = db["preferences"].find_one({"doc_type" : "last_check"})
    last_checked_at = ans["checked_at"]
    orig_checked_at = last_checked_at
    last_check = last_checked_at.strftime("%Y-%m-%d %H:%M:%S")
    tot_processed = 0
    new_recs = []
    max_time = orig_checked_at
    # Timer will trigger every 2 minutes, for 10 sec operation run 12 times
    for iter in range(3):
        query =   f"""
        select * from `{warehouse_db}` 
        where modified_at > datetime("{last_check}") 
        order by modified_at DESC 
        limit 100"""
        print(query)

        query_job = client.query(query)

        results = query_job.result()  # Waits for job to complete.
        ids = []
        print("#---------------- RESULTS ---------------#")
        print(f"  run: {last_check}")
        for item in results:
            if item["modified_at"] > max_time:
                max_time = item["modified_at"]
            doc = dict(item)
            doc["doc_type"] = "provider_change"
            print(doc)
            new_recs.append(doc)
            ids.append(item["provider_id"])
            tot_processed += 1
        answer = ",".join(ids)
        print(answer)
        time.sleep(10)
        last_checked_at = datetime.datetime.now() #last_checked_at + datetime.timedelta(seconds=10)
        last_check = last_checked_at.strftime("%Y-%m-%d %H:%M:%S")
        if len(new_recs) > 0:
            db["change_activity"].insert_many(new_recs)
            new_recs = []
    db["preferences"].update_one({"doc_type" : "last_check"},{"$set" : {"checked_at" : last_checked_at}})
    db["activity_log"].insert_one({"activity" : "data polling", "checked_at": orig_checked_at, "processed" : tot_processed})
    return Response({'message': 'successfully connected'}, status=200, mimetype='application/json')

def thread_count():
    # read settings and echo back
    multiprocessing.set_start_method("fork", force=True)
    passed_args = {"ddl_action": "info"}
    settings = get_settings()
    num_procs = settings["process_count"]
    jobs = []
    inc = 0
    for item in range(num_procs):
        p = multiprocessing.Process(target=worker_load, args=(item, passed_args))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    print("Main process is %s %s" % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def worker_load(ipos, args):
    #  Reads EMR sample file and finds values
    cur_process = multiprocessing.current_process()
    pid = cur_process.name.replace("rocess","")
    settings = get_settings()
    batches = settings["batches"]
    batch_size = settings["batch_size"]
    base_counter = settings["base_counter"] + batch_size * batches * ipos
    prefix = "QC-"
    IDGEN = Id_generator({"seed": base_counter})
    print(f"# -------------- ({cur_process.name} - {ipos}) Loading Synth Data ------------------ #")
    count = batch_size * batches
    IDGEN.set({"seed": base_counter, "size": count, "prefix": prefix})
    for b in range(batches):
        for cnt in range(batch_size):
            cur_id = IDGEN.get(prefix)
            print(f'[{pid}] - {cur_id}, batch: {b}-{cnt}')

 
# ---------------------------------- #
#claim_polling_trigger({})

thread_count()