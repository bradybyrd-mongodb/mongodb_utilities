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
import getopt
import bson
from bson.objectid import ObjectId
from bson.json_util import dumps
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from faker import Faker
base_dir = os.path.dirname(os.path.abspath(__file__))
# apppend parent folder to path
sys.path.append(os.path.dirname(base_dir))
from bbutil import Util
from id_generator import Id_generator

fake = Faker()
letters = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
providers = ["cigna","aetna","anthem","bscbsma","kaiser"]

'''
 #  Recommendations Engine

Load lookup for product recommendations
#  BJB 7/11/22

    python3 recommendations.py action=load_csv

# Startup Env:
    Atlas M10BasicAgain

#  Methodology

    import csv of recommendations
    create customer dataset
    create product dataset
    create new shelfs of recommendations - embed in customer doc    
'''
settings_file = "recommendations_settings.json"

def load_recommendations_data():
    # read settings and echo back
    bb.message_box("Recommendations Loader", "title")
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
    MASTER_CUSTOMERS = []
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    #file_log(f'New process {cur_process.name}')
    start_time = datetime.datetime.now()
    if ipos == 0:
        worker_csv_load()
    #worker_claim_load(pgcon,tables)
    #worker_rx_load(pgcon,tables)
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    #file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def worker_csv_load():
    #  Reads csv file and finds values
    collection = 'suggestions'
    prefix = "REC"
    count = 1000000
    conn = client_connection()
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = settings["batch_size"]
    IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefix})
    csv_file = settings["import_csv"]
    headers = []
    bulk_docs = []
    with open(csv_file) as csvfile:
        propreader = csv.reader(csvfile)
        icnt = 0
        for row in propreader:
            if icnt == 0:
                headers = row
            else:
                #print(f'{headers[0]}: {row[0]}, {headers[1]}: {row[1]}, {headers[2]}: {row[2]}')
                if icnt % batch_size == 0:
                    db[collection].insert_many(bulk_docs)
                    bb.logit(f'Adding {batch_size} total: {icnt}')
                    bulk_docs = []
                doc = {headers[0]: row[0], headers[1]: row[1], headers[2]: row[2], headers[3]: row[3]}
                #append_customer_info(doc)
                bulk_docs.append(doc)
            icnt += 1
        # get the leftovers
        db[collection].insert_many(bulk_docs)
                      
def worker_customer_load():
    #  Creates new customer profile
    collection = 'profiles'
    prefix = "CUS"
    conn = client_connection()
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = settings["batch_size"]
    num_records = 10000
    start = datetime.datetime.now()
    IDGEN.set({"seed" : base_counter, "size" : num_records, "prefix" : prefix})
    base_id = int(IDGEN.get(prefix, num_records).replace(prefix,""))
    bulk_docs = []
    icnt = 0
    bstart = datetime.datetime.now()
    for row in range(num_records):
        new_id = f'{prefix}{base_id + icnt}'
        if icnt != 0 and icnt % batch_size == 0:
            db[collection].insert_many(bulk_docs)
            bb.logit(f'Adding {batch_size} total: {icnt}')
            print_stats(bstart, batch_size)
            bstart = datetime.datetime.now()
            bulk_docs = []
        bulk_docs.append(customer_doc(new_id))
        icnt += 1
    # get the leftovers
    db[collection].insert_many(bulk_docs)
    cur_process = multiprocessing.current_process()
    bb.logit("#-------- COMPLETE -------------#")
    print_stats(start, icnt)

def customer_doc(idval):     
    age = random.randint(28,84)
    year = 2020 - age
    month = random.randint(1,12)
    day = random.randint(1,28)
    name = fake.name()
    doc = {}
    doc['profile_id'] = idval
    doc['birth_date'] = datetime.datetime(year,month,day, 10, 45)
    doc['first_name'] = name.split(" ")[0]
    doc['last_name'] = name.split(" ")[1]
    doc['phone'] = fake.phone_number()
    doc['email'] = f'{name.replace(" ",".")}@randomfirm.com'
    doc["gender"] = random.choice(["M","F"])
    doc["address1_type"] = "work"
    doc["address1_street"] = fake.street_address()
    doc["address1_line2"] = ""
    doc["address1_city"] = fake.city()
    doc["address1_state"] = fake.state_abbr()
    doc["address1_zipcode"] = fake.zipcode()
    doc["recommendations"] = []
    doc["recent_purchases"] = []
    doc["version"] = "1.0"
    return(doc)

def worker_sku_update():
    #  Reads csv file and finds values
    collection = 'products'
    prefix = "SKU"
    count = 1000000
    conn = client_connection()
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = 500
    IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefix})
    base_id = int(IDGEN.get(prefix, 10000).replace(prefix,""))
    bulk_docs = []
    cur = db[collection].find({})
    icnt = 0
    for row in cur:
        new_id = f'{prefix}{base_id + icnt}'
        shortname = row["product_name"]
        modelno = row["model_number"] if "model_number" in row else random.randint(10000,99999)
        shortname = f'{shortname.replace(" ","")[0:20]}_{new_id}'
        sku = f'{shortname[0:2]}-{modelno}-{new_id}'
        if icnt != 0 and icnt % batch_size == 0:
            bb.logit(f'Adding {batch_size} total: {icnt}, bsize: {len(bulk_docs)}')
            bulk_writer(db[collection], bulk_docs)
            bulk_docs = []
                    
        bulk_docs.append(
            UpdateOne({"_id" : row["_id"]},{"$set": {"short_name": shortname, "sku" : sku, "product_id" : new_id}})
        )
        icnt += 1
    # get the leftovers
    bulk_writer(db[collection], bulk_docs)

def bulk_writer(collection, bulk_arr):
    try:
        result = collection.bulk_write(bulk_arr)
        ## result = db.test.bulk_write(bulkArr, ordered=False)
        # Opt for above if you want to proceed on all dictionaries to be updated, even though an error occured in between for one dict
        pprint.pprint(result.bulk_api_result)
    except BulkWriteError as bwe:
        print("An exception occurred ::", bwe.details)

def worker_load_recommendations(justShelf = 0):
    #  Build recommendatino shelfs for user
    collection = 'profiles'
    alt_coll = 'products'
    prefix = "REC"
    count = 1000000
    if "shelf" in ARGS:
        justShelf = int(ARGS["shelf"])
    conn = client_connection()
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = 500
    start = datetime.datetime.now()
    IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefix})
    base_id = int(IDGEN.get(prefix, 10000).replace(prefix,""))
    bulk_docs = []
    bulk_recs = []
    if justShelf == 0:
        pipe = [{"$project" : {"profile_id" : 1}}]
    else:
        pipe = [{"$sample" : {"size" : justShelf}},{"$project" : {"profile_id" : 1}}]
    cur = db[collection].aggregate(pipe)
    icnt = 0
    bstart = datetime.datetime.now()
    for row in cur:
        new_id = f'{prefix}{base_id + icnt}'
        '''
        Build shelf of products here
        product - associate with product - 20 suggestions ranked per shelf
        '''
        if icnt != 0 and icnt % batch_size == 0:
            bb.logit(f'Adding {batch_size} total: {icnt}, bsize: {len(bulk_docs)}')
            bulk_writer(db[collection], bulk_docs)
            print_stats(bstart, batch_size)
            bstart = datetime.datetime.now()
            bulk_docs = []
        
        if justShelf == 0:
            ans = get_recent_purchases(db[alt_coll])
        recs_ans = get_recent_purchases(db[alt_coll], True)
        recs = []
        for item in recs_ans["items"]:
            item.pop("purchased_at", None)
            item["profile_id"] = row["profile_id"]
        if justShelf == 0:
            bulk_docs.append(
                UpdateOne({"_id" : row["_id"]},{"$set": {"recent_purchases": ans}, "$addToSet" : {"recommendations" : recs_ans}})
            )
        else:
            bulk_docs.append(
                UpdateOne({"_id" : row["_id"]},{"$addToSet" : {"recommendations" : recs_ans}})
            )
                
        icnt += 1
    # get the leftovers
    bulk_writer(db[collection], bulk_docs)
    bb.logit("#-------- COMPLETE -------------#")
    print_stats(start, icnt)

def get_recent_purchases(coll, isShelf = False):
    # return 20 random items
    pipe = [{"$sample" : { "size" : 20}},{"$project": {"sku": 1, "product_name": 1, "short_name": 1}}]
    res = coll.aggregate(pipe)
    result = []
    icnt = 0
    for item in res:
        buy_at = fake.date_time_between('-2y', datetime.datetime.now())
        result.append({"sku" : item["sku"],"product_name" : item["product_name"], "short_name" : item["short_name"], "purchased_at": buy_at, "rank" : icnt})
        icnt += 1
    if isShelf:
        shelf = {"category" : random.choice(["retail","pharmacy","health"]), "created_at" : datetime.datetime.now(), "items" : result}
        return shelf
    else:
        return result

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

def print_stats(start_t, docs = 0):
    end = datetime.datetime.now()
    elapsed = end - start_t
    secs = (elapsed.seconds) + elapsed.microseconds * .000001
    bb.logit(f"Elapsed: {secs} - Docs: {docs}")

def query_mix():
    # mixed query
    conn = client_connection()
    db = conn[settings["database"]]
    mix = ARGS["mix"]
    pipe = [
    {
        '$sample': {
            'size': 20
        }
    }, {
        '$unwind': {
            'path': '$recommendations'
        }
    }, {
        '$match': {
            'recommendations.category': 'pharmacy'
        }
    }, {
        '$project': {
            'profile_id': 1, 
            'first_name': 1, 
            'last_name': 1, 
            'shelfDate': '$recommendations.created_at', 
            'items': '$recommendations.items'
        }
    }
    ]
    rtot = 0
    wtot = 0
    for k in range(20):
        start = datetime.datetime.now()
        if mix == "high":
            inum = 40
        elif mix == "low":
            inum = 10
        else:
            inum = 20
        worker_load_recommendations(inum)
        for k in range(20):
            db.profiles.aggregate(pipe)
        print_stats(start, inum + 20)


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

#------------------------------------------------------------------#
#     MAIN
#------------------------------------------------------------------#
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
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
        load_recommendations_data()
    elif ARGS["action"] == "load_skus":
        worker_sku_update()
    elif ARGS["action"] == "customer_load":
        worker_customer_load()
    elif ARGS["action"] == "recommendations_load":
        worker_load_recommendations()
    elif ARGS["action"] == "query_mix":
        query_mix()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()

'''
[
    {
        '$group': {
            '_id': '$MASKED_XCC', 
            'CustId': {
                '$first': '$MASKED_XCC'
            }, 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'count': -1
        }
    }
]
'''