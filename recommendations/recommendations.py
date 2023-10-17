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
#from threading import Thread
#from threading import Lock
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
        p = multiprocessing.Process(target=worker_load, args = (item,params))
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

def build_csv_data():
    #  Creates csv files
    #  20Million customers, 1 billion recommends (100/customer)
    '''
        create customer, create 100 recommends
    '''
    collection = 'suggestions'
    prefix = "REC"
    count = 1000000
    conn = client_connection()
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = settings["batch_size"]
    IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefix})
    csv_file = settings["import_csv"]
    fields = []
    bulk_docs = []
    icnt = 0
    for batchnum in range(batches):
        for it in range(batch_size):
            buy_at = fake.date_time_between('-2y', datetime.datetime.now())
            bulk_docs.append({"sku" : item["sku"],"product_name" : item["product_name"], "short_name" : item["short_name"], "purchased_at": buy_at, "rank" : icnt})
        
            doc = {headers[0]: row[0], headers[1]: row[1], headers[2]: row[2], headers[3]: row[3]}
            #append_customer_info(doc)
            bulk_docs.append(doc)
            icnt += 1
        # get the leftovers
        db[collection].insert_many(bulk_docs)

def write_dict(fields,dict, filepath):
    #  write the dict to file
    fieldnames = []
    writer = csv.DictWriter(filepath, fieldnames = fieldnames)
    writer.writeheader()
    writer.writerows(dict)


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

#-----------------------------------------------------------------------#
#  POC Data Analysis
#-----------------------------------------------------------------------#

def build_recommendation_data():
    products = []
    count = 1000000
    conn = client_connection()
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = 500
    collection = 'sku_attributes'
    prefix = "PROD"
    segments = market_segments(batch_size)
    IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefix})
    base_id = int(IDGEN.get(prefix, batch_size).replace(prefix,""))
    for item in range(batch_size):
        new_id = f'{prefix}{base_id + item}'
        new_doc = product_doc(new_id, item)
        new_doc["market_segment_ranks"] = update_segment_ranks(segments, batch_size)
        products.append(new_doc)
        bb.logit(f'created {new_doc["product_name"]}')
    for item in range(batch_size):
        products[item]["bought_together"] = bought_together(products)

    db[collection].insert_many(products)
    #  Now xtracard details
    batch_size = batch_size * 10
    collection = 'xtra_card'
    prefix = "XTRA"
    xtras = []
    IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefix})
    base_id = int(IDGEN.get(prefix, batch_size).replace(prefix,""))
    for item in range(batch_size):
        new_id = f'{prefix}{base_id + item}'
        new_doc = xtra_card_doc(new_id, item)
        mkt_segment = segments[random.randint(1,len(segments)-1)]
        new_doc["market_segment"] = xtra_market_segment(mkt_segment)
        new_doc["recommendations"] = recommendations(products)
        new_doc["coupon_ranks"] = xtra_coupon_rank()
        xtras.append(new_doc)
        bb.logit(f'created {new_doc["last_name"]} xtra')
    db[collection].insert_many(xtras)
    

def xtra_market_segment(segment):
    age = random.randint(28,84)
    year = 2020 - age
    month = random.randint(1,12)
    day = random.randint(1,28)
    xtra_segment = {}
    xtra_segment["market_cd"] = segment["market_cd"]
    xtra_segment["earliest_purch_dt"] = datetime.datetime(year - 2,month,day, 10, 45)
    xtra_segment["latest_purch_dt"] = datetime.datetime(year,month,day, 10, 45)
    xtra_segment["home_store_nbr"] = random.randint(100000,110000)
    xtra_segment["last_mod_dt"] = datetime.datetime.now() 
    return(xtra_segment)

def xtra_coupon_rank():
    ranks = []
    for it in range(100):
        doc = {}
        rank = random.randint(1,100)
        pool_id = f'POOL-{random.randint(100000,1000000)}'
        doc['offer_pool_id'] = pool_id
        doc['rank_nbr'] = rank
        doc["last_mod_dt"] = datetime.datetime.now()
        ranks.append(doc)
    return(ranks)

def product_doc(idval, seq):     
    age = random.randint(28,84)
    year = 2020 - age
    month = random.randint(1,12)
    day = random.randint(1,28)
    name = fake.bs()
    doc = {}
    doc['sku_nbr'] = idval
    doc['product_name'] = name
    doc["last_mod_dt"] = datetime.datetime(year,month,day, 10, 45)
    doc['vendor_nbr'] = fake.phone_number()
    doc["version"] = "1.0"
    return(doc)

def market_segments(num_prods):
    segments = []
    for city in range(100):
        name = fake.city()
        doc = {}
        doc['market_cd'] = name
        rank = random.randint(1,num_prods)
        doc['sku_rank_nbr'] = rank
        doc['sku_rank_score'] = rank/100
        doc["last_mod_dt"] = datetime.datetime.now()
        segments.append(doc)
    bb.logit(f'created 100 market segments')
    return(segments)

def bought_together(products):
    siz = len(products) - 1
    affinities = []
    for cnt in range(10):
        doc = {}
        prod = products[random.randint(1,siz)]
        doc["sku_nbr"] = prod["sku_nbr"]
        doc["name"] = prod["name"]
        doc["rank"] = random.randint(1,10)
        affinities.append(doc)
    bb.logit(f'created {10} bought-togethers')
    return(affinities)

def update_segment_ranks(segs, num_prods):
    for it in range(len(segs)):
        rank = random.randint(1,num_prods)
        segs[it]['sku_rank_nbr'] = rank
        segs[it]['sku_rank_score'] = rank/100
    return(segs)

def xtra_card_doc(idval, seq):     
    age = random.randint(28,84)
    year = 2020 - age
    month = random.randint(1,12)
    day = random.randint(1,28)
    name = fake.name()
    doc = {}
    doc['xtra_card_nbr'] = idval
    doc['first_name'] = name.split(" ")[0]
    doc['last_name'] = name.split(" ")[1]
    doc["last_mod_dt"] = datetime.datetime.now()
    doc["version"] = "1.0"

    return(doc)

def recommendations(products):
    shelf = 60
    shelf_no = 0
    siz = len(products) - 1
    recommendations = []
    for cnt in range(shelf):
        if cnt % 20 == 0:
            shelf_no += 1
        doc = {}
        prod = products[random.randint(1,siz)]
        doc["shelf_nbr"] = shelf_no
        doc["sku_nbr"] = prod["sku_nbr"]
        doc["name"] = prod["product_name"]
        doc["rank"] = random.randint(1,shelf)
        doc["rank_type_cd"] = random.choice(["geo", "affinity", "other"])
        recommendations.append(doc)
    bb.logit(f'created {shelf} recommendations')
    return(recommendations)

def update_bought():
    conn = client_connection()
    db = conn[settings["database"]]
    bb.logit("Update Bought")
    collection = 'product_skus'
    cur = db[collection].find({},{"_id": 1, "sku_nbr" : 1, "name": 1})
    products = list(cur)
    cnt = 0
    for item in products:
        cur_id = item["_id"]
        ans = bought_together(products)
        if cnt % 10 == 0:
            bb.logit(f'Updating: {cnt}')
        db[collection].update_one({"_id" : cur_id},{"$set" : {"bought_together" : ans}})
        cnt += 1


#-----------------------------------------------------------------------#
#  Building combined extracard data
#-----------------------------------------------------------------------#

def load_enriched_data():
    # read settings and echo back
    bb.message_box("Enriched Loader", "title")
    bb.logit(f'# Settings from: {settings_file}')
    # Spawn processes
    num_procs = settings["process_count"]
    jobs = []
    seed_id = 8000000
    inc = 0
    params = {"file_lock" : g_lock, "file_handle": g_logger}
    #multiprocessing.set_start_method("fork", force=True)
    for item in range(num_procs):
        p = multiprocessing.Process(target=xtracard_merger_all, args = (item,seed_id,num_procs,params))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def xtracard_merger_all(procseq, seed_id, num_procs, details):
    #  Reads csv file and finds values
    settings_file = "recommendations_settings.json"
    collection = 'extra_card_new'
    source_coll = 'xtra_card'
    alt_coll = 'xtra_card_mkt_sgmt'
    alt2_coll = 'xtra_card_sku_rank'
    key = "XTRA_CARD_NBR"
    bb = Util()
    settings = bb.read_json(settings_file)
    locker = details["file_lock"]
    hfile = details["file_handle"]
    cur_process = multiprocessing.current_process()
    procid = cur_process.name.replace("Process", "p")
    tstart_time = datetime.datetime.now()
    conn = client_connection("uri", settings)
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    #if procseq == 0:
    #    start_val = 464977
    increment = 1000000
    base_card_nbr = procseq * increment + seed_id
    base_card_limit = seed_id + (procseq + 1) * increment
    batch_size = 1000
    msg = f'[{procid}] Starting - from {base_card_nbr} to {base_card_limit}'
    file_log(msg,locker,hfile)
    bb.message_box(msg)
    totcnt = 0
    cnt = 0
    bulk_docs = []
    bulk_ids = []
    for batchinc in range(1000):
        bstart_time = datetime.datetime.now()
        pipe = [
            {"$match" : {"$and": [{key: {"$gt": base_card_nbr}}, {key: {"$lte": base_card_limit}}]}},
            {"$skip" : batch_size * batchinc},
            {"$limit" : batch_size}
        ]
        bb.logit(f"[{procid}] Building Pipe: {base_card_nbr} - {base_card_limit}")
        #pprint.pprint(pipe)
        cur = db[source_coll].aggregate(pipe)
        # This gets each batch of 1000 xtra_cards
        bulk_docs = []
        bulk_ids = []
        for row in cur:
            row["version"] = "1.1"
            bulk_docs.append(row)
            bulk_ids.append(row[key])
        if len(bulk_ids) < 1:
            break
        # Now match up the associated market segments
        cur = db[alt_coll].find({key : {"$in" : bulk_ids}})
        cnt = 0
        for row in cur:
            card_id = row[key]
            row.pop("_id", None)
            ipos = in_list(bulk_ids,card_id)
            if ipos > 0:
                bulk_docs[ipos]["market_segment"] = row
            totcnt += 1
            cnt += 1
        bb.logit(f'[{procid}] sgmts = {cnt} in {len(bulk_ids)} cards')
        result = query_related_skus(procid,db[alt2_coll],bulk_ids,bulk_docs, totcnt)

        bb.logit(f'{len(bulk_docs)} to insert')
        db[collection].insert_many(bulk_docs)
        bb.logit(f"[{procid}] Processed: {totcnt} batch: {batchinc} - in {timer(bstart_time)} secs")
        
    # get the leftovers
    if len(bulk_docs) > 0:
        bb.logit("Saving last records")
        db[collection].insert_many(bulk_docs)
    msg = f'[{procid}] Completed batch: {base_card_nbr} - {base_card_limit}, {cnt} cards'
    file_log(msg, locker, hfile)
    bb.logit(msg)
    timer(tstart_time, False)
    bb.logit(f"[{procid}] # -------------- COMPLETE ------------------- #")

def query_related_skus(procid,dbcoll, bulk_ids, bulk_docs, tot):
    # Now pull the market skus
    # Note- may be 100 skus per xtra card - meaning 60-70k records returned
    key = "XTRA_CARD_NBR"
    cnt = 0
    tcnt = 0
    ilen = len(bulk_ids)
    skus = []
    inc = 100
    for skubatch in range(10):
        sval = skubatch * inc
        fval = (skubatch + 1) * inc
        if sval > ilen:
            break
        if fval > ilen:
            fval = ilen - 1
        cur = dbcoll.find({key : {"$in" : bulk_ids[sval:fval]}}).sort([(key, 1)]) 
        last_id = 0
        card_id = 0
        #print(f'[{procid}] skubatch: {sval}-{fval}')
        cnt = 0  
        for row in cur:
            card_id = row[key]
            if cnt > 0 and last_id != card_id:
                ipos = in_list(bulk_ids,card_id)
                if ipos > 0:
                    bulk_docs[ipos]["recommended_skus"] = skus
                skus = []
                last_id = card_id
            row.pop("_id", None)
            skus.append(row)
            tot += 1
            cnt += 1
            tcnt += 1
    print(f'[{procid}] skus = {tcnt}')
    if len(skus) > 0:
        ipos = in_list(bulk_ids,card_id)
        if ipos > 0:
            bulk_docs[ipos]["recommended_skus"] = skus
    return bulk_docs

# -------------------------------------------------------------------- #
# -------  File-based update
#  Chunkify
    
def update_file():
    source_file = "../../../customers/CVS/extraCare/poc_data/sku_rank/mongo_poc_xtra_card_sku_rank.dat.gz.partaa"
    params = {
        #"source_file" : "f:\\pocdata\\mongo_poc_xtra_card_market_segment.dat"
        #"source_file" : "../../../customers/CVS/extraCare/poc_data/mongo_poc_xtra_card_market_segment.dat",
        "source_file" : "../../../customers/CVS/extraCare/poc_data/sku_rank/mongo_poc_xtra_card_sku_rank.dat.gz.partaa",
        #"file_type" : "market_segment"
        "file_type" : "sku_rank",
        "update_format" : "array",
        "file_lock" : g_lock, 
        "file_handle": g_logger
    }
    if "file" in ARGS:
        params["source_file"] = ARGS["file"]
    chunk_size = 1024*1024
    cores = 4
    #init objects
    pool = multiprocessing.Pool(cores)
    jobs = []
    proc_cnt = 0
    fsize = int(os.path.getsize(params["source_file"])/(1024*1024))
    msg = f'FileUpdater: {fsize} Mb, size: {chunk_size}'
    bb.message_box(msg,"title")
    file_log(msg,g_lock,g_logger)
    bb.logit(f'  File: {params["source_file"]}')
    file_log(f'  File: {params["source_file"]}',g_lock,g_logger)
    #create jobs
    num_procs = settings["process_count"]
    jobs = []
    chunk_count = 0
    chunk_sum = 0
    chunk_pos = 0
    inc = 0
    #multiprocessing.set_start_method("fork", force=True)
    for chunk_start,chunk_size in file_chunkify(params["source_file"], chunk_size):
        p = multiprocessing.Process(target=extra_card_merger_update, args = (proc_cnt, chunk_start, chunk_size, params))
        jobs.append(p)
        p.start()
        time.sleep(1)
        proc_cnt += 1
        chunk_count += 1
        chunk_sum += chunk_size
        chunk_pos = chunk_start
        inc += 1
        if inc == 100:
            msg = f'Progress chunks: {chunk_count}, totalbytes: {chunk_sum}, filepos: {chunk_pos}'
            file_log(msg,locker,hfile)
            inc = 0
    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()
    '''
    for chunk_start,chunk_size in file_chunkify(params["source_file"], chunk_size):
        jobs.append( pool.apply_async(extra_card_merger_update,(proc_cnt, chunk_start, chunk_size, params)) )
        proc_cnt += 1
    #wait for all jobs to finish
    for job in jobs:
        job.get()
    #clean up
    pool.close()
    '''

# From : https://www.blopig.com/blog/2016/08/processing-large-files-using-python/
def file_chunkify(fname, size=1024):
    fileEnd = os.path.getsize(fname)
    num_chunks = 0
    with open(fname,'rb') as f:
        chunk_end = f.tell()
        while True:
            chunk_start = chunk_end
            f.seek(size,1)
            f.readline()
            chunk_end = f.tell()
            yield chunk_start, chunk_end - chunk_start
            num_chunks += 1
            print(f'Dividing file: {num_chunks} chunks')
            if chunk_end > fileEnd:
                break

def extra_card_merger_update(proc_seq, chunk_start, chunk_size, details):
    '''
        Read from file (start multiple procs for each different file)
        for each row in file updateOne  $set item
    '''
    #  Reads csv file and finds values
    settings_file = "recommendations_settings.json"
    collection = 'xtra_card'
    file_type = details["file_type"]
    is_array = False
    if details["update_format"] == "array":
        is_array = True
    locker = details["file_lock"]
    hfile = details["file_handle"]
    key = "XTRA_CARD_NBR"
    bb = Util()
    settings = bb.read_json(settings_file)
    cur_process = multiprocessing.current_process()
    procid = cur_process.name.replace("Process", "p")
    tstart_time = datetime.datetime.now()
    bstart_time = datetime.datetime.now()
    conn = client_connection("uri", settings)
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = 1000
    totcnt = 0
    cnt = 0
    b_cnt = 0
    oper = "$set"
    if is_array:
        oper = "$addToSet"               
    msg = f'[{procid}] #--------------------- Starting ------------------------#'
    file_log(msg,locker,hfile)           
    msg = f'[{procid}] Starting batch ({batch_size}) total {totcnt}'
    file_log(msg,locker,hfile)
    bb.message_box(msg)
    bulk_updates = []
    with open(details["source_file"]) as f:
        f.seek(chunk_start)
        lines = f.read(chunk_size).splitlines()
        for line in lines:
            sub_doc = {"version" : "1.1"}
            row = line.split(",") #line.split("|")
            card_id = int(row[0])
            sub_doc = process_file_row(sub_doc, row, file_type)
            bulk_updates.append(
                UpdateOne({key : card_id},{oper: {file_type : sub_doc}, "$set" : {"updated_at" : datetime.datetime.now(), "updated_by" : f'sku_rank-{procid}'}})
            )
            cnt += 1
            totcnt += 1
            if cnt == batch_size:
                bulk_writer(db[collection], bulk_updates, f'[{procid}] lastId: {card_id}')
                bb.logit(f"[{procid}] Processed: {totcnt} batch: {b_cnt} - in {timer(bstart_time)} secs")
                b_cnt += 1
                bstart_time = datetime.datetime.now()
                bulk_updates = []
                cnt = 0

        # get the leftovers
        if len(bulk_updates) > 0:
            bulk_writer(db[collection], bulk_updates)
            bb.logit(f'Final batch {len(bulk_updates)} to process')
    msg = f'[{procid}] Completed batch: {b_cnt} - {totcnt} cards'
    file_log(msg,locker,hfile)
    bb.logit(msg)
    timer(tstart_time, False)
    bb.logit("# -------------- COMPLETE ------------------- #")

# -------------------------------------------------------------------- #
# -------  Multi_threaded Export 11/29/22
    
def export_filer():
    params = {
        "export_file" : "sku_rank_exp",
        "export_dir" : "../export",
        "num_procs" : 20,
        "chunks" : 1000,
        "omax_id" : 520000000,
        "max_id" : 100000000,
        "min_id" : 50000000,
        "last" : "n",
        "collection" : "xtra_card_sku_rank"
    }
    if "export_params" in settings:
        params = settings["export_params"]
    if "file" in ARGS:
        params["source_file"] = ARGS["file"]
    params["file_lock"] = g_lock
    params["file_handle"] = g_logger
    params["chunk_size"] = int((params["max_id"] - params["min_id"])/params["chunks"])
    jobs = []
    proc_cnt = 0
    msg = f'Starting Exporter: size: {params["chunk_size"]}, Files: {params["num_procs"]}'
    bb.message_box(msg,"title")
    file_log(msg, g_lock, g_logger)
    #create jobs
    num_procs = params["num_procs"] #settings["process_count"]
    jobs = []
    inc = 0
    for item in range(num_procs):
        if item == num_procs - 1:
            params["last"] = "y"
        p = multiprocessing.Process(target=export_processor, args = (proc_cnt, params))
        jobs.append(p)
        p.start()
        time.sleep(1)
        proc_cnt += 1
    main_process = multiprocessing.current_process()
    msg = '#----------------------------- All Complete ---------------------------------#'
    file_log(msg, g_lock, g_logger)
    msg = f'Finish Exporter: size: {params["min_id"]}-{params["max_id"]}, Procs: {params["num_procs"]}, Chunks: {params["chunks"]}'
    file_log(msg, g_lock, g_logger)
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    g_logger.close()
    for i in jobs:
        i.join()



def bought_toghter():
    ans = db.collection.find({sku: 23456})
    return(ans["bought_together"])



def chunk_range(min,max,chunk_size, chunk_num):
    min = chunk_num * chunk_size
    max = min + chunk_size
    return min, max   


def export_processor(proc_seq, details):
    '''
        Look through chunks appending to file
        see: https://gist.github.com/mieitza/5d35d0a4f2671127f7120c75c8764385
    '''
    settings_file = "recommendations_settings.json"
    bb = Util()
    settings = bb.read_json(settings_file)
    cur_process = multiprocessing.current_process()
    procid = cur_process.name.replace("Process", "p")
    collection = details["collection"]
    minid = details["min_id"]
    maxid = details["max_id"]
    tot_chunks = details["chunks"]
    chunk_size = details["chunk_size"]
    num_chunks = int(tot_chunks/details["num_procs"])
    locker = details["file_lock"]
    hfile = details["file_handle"]
    if details["last"] == "y":
        num_chunks = num_chunks + 0
    filename = f'{details["export_dir"]}/{details["export_file"]}_{procid}.csv'
    tstart_time = datetime.datetime.now()
    bstart_time = datetime.datetime.now()
    conn = client_connection("uri", settings)
    db = conn[settings["database"]]
    base_chunk = proc_seq * num_chunks
    end_chunk = base_chunk + num_chunks
    totcnt = 0
    cnt = 0
    b_cnt = 0
    msg = f'[{procid}] #--------------------- Starting ------------------------#'
    file_log(msg,locker,hfile)          
    msg = f'[{procid}] Starting: {num_chunks} to do of size: {chunk_size} - total {totcnt}'
    file_log(msg,locker,hfile)
    bb.message_box(msg)
    bulk_updates = []
    with open(filename, "a") as fil:
        for chunk in range(base_chunk, end_chunk):
            start_id = minid + chunk * chunk_size
            end_id = start_id + chunk_size
            if chunk == end_chunk and details["last"] == "y":
                end_id = maxid
            cursor = db[collection].find({"$and": [{"XTRA_CARD_NBR": {"$gt": start_id}}, {"XTRA_CARD_NBR": {"$lte": end_id}}]})
            df =  pd.DataFrame(list(cursor))
            csize = len(df)
            msg = f'[{procid}] Batch: {chunk} - rows: {csize}, range: {start_id}-{end_id}, total: {totcnt}'
            bb.logit(msg)
            file_log(msg,locker,hfile)
            if csize > 0:
                del df['_id']
                df.to_csv(filename, mode='a', index=False)
                totcnt += csize
            lst = [df]
            del df
            del lst
    msg = f'[{procid}] Completed process: {totcnt} items'
    file_log(msg,locker,hfile)
    bb.logit(msg)
    timer(tstart_time, False)
    bb.logit("# -------------- COMPLETE ------------------- #")

#------------------------------------------------------------#
#-------------------------------------------------------------#
# From : python3 recommendations.py action=file_restart from=4122,80942344 file=../sku_rank_csv/mongo_poc_xtra_card_sku_rank.datai.csv
def file_restart():
    if "file" in ARGS:
        fname = ARGS["file"]
    if "from" in ARGS:
        frompt = ARGS["from"]
    it = frompt.split(",")
    stop_chunk = int(it[0])
    card_id = int(it[1])
    fileEnd = os.path.getsize(fname)
    est_pos = 1024*1024*stop_chunk
    num_chunks = 0
    bb.logit(f"Restarting from chunk: {stop_chunk} card_nbr: {card_id}")
    with open(fname,'rb') as f:
        chunk_end = f.tell()
        found = ""
        f.seek(est_pos,1)
        line = f.readline()
        print(f'exact: {line}')
        f.seek(est_pos - 1024,1)
        for inc in range(100):
            line = f.readline()
            items = line.split(',')
            cur_card = int(items[0])
            if card_id == cur_card:
                found = "-found!"
            print(f'Found({inc}):{cur_card} - {line}')
            chunk_end = f.tell()
        
        

#------------------------------------------------------------------------#

def process_file_row(doc,row,ftype):
    if ftype == "market_segment":
        doc["MARKET_CD"] = row[1]
        doc["SEG_ID"] = int(row[2])
        doc["AD_DT"] = datetime.datetime.strptime(row[3], "%Y-%m-%d")
        doc["AD_VERSION_CD"] = row[4]
        doc["LATEST_PURCH_DT"] = datetime.datetime.strptime(row[5], "%Y-%m-%d")
        doc["EARLIEST_PURCH_DT"] = datetime.datetime.strptime(row[6], "%Y-%m-%d")
        ival = 0
        if row[7] != '':
            ival = int(row[7])
        doc["HOME_STORE_NBR"] = int(ival)
        doc["TGT_GEO_MKT_CD"] = row[8]
    elif ftype == "sku_rank":
        doc["RANK_TYPE_CD"] = row[1]
        doc["SKU_NBR"] = int(row[2])
        doc["SKU_RANK_NBR"] = int(row[3])
        doc["SKU_RANK_SCORE"] = float(row[4])
        doc["CREATE_DTTM"] = datetime.datetime.strptime(row[5], "%Y-%m-%d %H:%M:%S")
    return(doc)
            
def xtracard_merger_update():
    #  Reads csv file and finds values
    collection = 'extra_card_new'
    source_coll = 'xtra_card'
    alt_coll = 'xtra_card_mkt_sgmt'
    alt2_coll = 'xtra_card_sku_rank'
    key = "XTRA_CARD_NBR"
    cur_process = multiprocessing.current_process()
    procid = cur_process.name.replace("Process", "p")
    conn = client_connection()
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    base_card_nbr = 0
    base_card_limit = 1000000
    batch_size = 10
    bb.message_box(f'[{procid}] Starting - from {base_card_nbr} to {base_card_limit}')
    totcnt = 0
    bulk_docs = []
    bulk_docs = []
    for batchinc in range(1000):
        pipe = [
            {"$match" : {"$and": [{key: {"$gt": base_card_nbr}}, {key: {"$lte": base_card_limit}}]}},
            {"$skip" : batch_size * batchinc},
            {"$limit" : batch_size}
        ]
        cur = db[source_coll].aggregate(pipe)
        bulk_docs = []
        bulk_ids = []
        for row in cur:
            row["version"] = "1.0"
            bulk_docs.append(row)
            bulk_ids.append(row[key])
        if len(bulk_ids) < 1:
            break
        cur = db[alt_coll].find({key : {"$in" : bulk_ids}})
        cur2 = db[alt2_coll].find({key : {"$in" : bulk_ids}})
        skus = cur2[:]
        bulk_updates = []
        cnt = 0
        for row in cur:
            card_id1 = row[key]
            ipos = bulk_ids.index(card_id1)
            if ipos > 0:
                cur = bulk_docs[ipos]
                bulk_updates.append(
                    UpdateOne({"_id" : cur["_id"]},{"$set": {"market_segment": row}})
                )
            totcnt += 1
            cnt += 1
        bb.logit(f'{len(update_docs)} to insert')
        bulk_writer(db[collection], update_docs)
        bb.logit(f"Processed: {totcnt} batch: {batchinc}")
        
    # get the leftovers
    if len(bulk_updates) > 0:
        bb.logit("Saving last records")
        bulk_writer(db[collection], bulk_updates)
    bb.logit("# -------------- COMPLETE ------------------- #")

def update_skus(params):
    '''
        Prep:
        db.createCollection("product_skus")
        db.product_skus.createIndex({"sku_nbr" : 1})
    '''
    collection = 'extra_card_full'
    tgt_coll = 'product_skus'
    cur_process = multiprocessing.current_process()
    procid = cur_process.name.replace("Process", "p")
    bb = Util()
    settings = bb.read_json(settings_file)
    tstart_time = datetime.datetime.now()
    bstart_time = datetime.datetime.now()
    conn = client_connection("uri", settings)
    db = conn[settings["database"]]
    cur_process = params["cur_process"]
    startid = params["boundaries"][cur_process]
    endid = params["boundaries"][cur_process + 1]
    batch_size = 1000
    increment = 2000
    totcnt = 0
    cnt = 0
    cdtot = 0
    bb.logit(f'[{procid}] #------------ Starting id={startid} - {endid} ------------------------#')
    skus = db[tgt_coll].distinct("sku_nbr")
    bulk_updates = []
    bulk_updates = []
    bb.logit(f'SKUs - {len(skus)} to do')
    for sku in skus:
        if totcnt < startid or totcnt > endid:
            totcnt += 1
            continue
        pipe = [
            {"$match" : {
                "sku_rank.SKU_NBR" : sku
            }},
            {"$project" : {"XTRA_CARD_NBR": 1, "market_segment" : 1, "sku_rank" : 1}},
            {"$skip" : 0},
            {"$limit" : increment},
            {"$unwind" : "$sku_rank"},
            {"$match" : {"sku_rank.SKU_NBR" : sku}}
        ]
        result = db[collection].aggregate(pipe)
        chg_doc = {}
        mkt_codes = []
        market_ranks = []
        for doc in result:
            new_doc = {}
            mkt_code = doc["market_segment"]["MARKET_CD"]
            if mkt_code in mkt_codes:
                #bb.logit(f'mktcd: {mkt_code} - found')
                continue
            else:
                mkt_codes.append(mkt_code)
                #bb.logit(f'mktcd: {mkt_code} - new')
            if len(mkt_codes) > 100:
                break
            new_doc["MARKET_CD"] = mkt_code
            new_doc["HOME_STORE_NBR"] = doc["market_segment"]["HOME_STORE_NBR"]
            new_doc["TGT_GEO_MKT_CD"] = doc["market_segment"]["TGT_GEO_MKT_CD"]
            new_doc["SKU_RANK_NBR"] = doc["sku_rank"]["SKU_RANK_NBR"]
            new_doc["SKU_RANK_SCORE"] = doc["sku_rank"]["SKU_RANK_SCORE"]
            new_doc["CREATE_DTTM"] = doc["sku_rank"]["CREATE_DTTM"]
            market_ranks.append(new_doc)
            cdtot += 1
        bought_together = []
        for inc in range(10):
            ival = random.randint(2000,40000)
            it = {"SKU_NBR" : skus[ival],"NAME" : fake.bs()}
            bought_together.append(it)
        bulk_updates.append(
            UpdateOne({"sku_nbr" : sku},{"$set": {"version" : "1.2", "market_ranks": market_ranks, 'name' : fake.bs(), 'manufacturer' : fake.company(), "bought_together" : bought_together}})
        )
        cnt += 1
        totcnt += 1
        if cnt == batch_size:
            bb.logit(f'Saving batch mkts - {cdtot}, skus - {totcnt}')
            bulk_writer(db[tgt_coll], bulk_updates)
            timer(bstart_time, False)
            bstart_time = datetime.datetime.now()
            bulk_updates = []
            cnt = 0
        msg = f'[{procid}] Completed item: {sku}: tot: {totcnt}, mktcd: {len(mkt_codes)}'
        bb.logit(msg)
    if len(bulk_updates) > 0:
        bulk_writer(db[tgt_coll], bulk_updates)
        bulk_updates = []
        cnt = 0
        
    bb.logit("# -------------- COMPLETE ------------------- #")
    timer(tstart_time, False)

def update_sku_master():
    jobs = []
    proc_cnt = 4
    params = {"boundaries" : [0,11000,22000,33000,44000]}
    num_procs = settings["process_count"]
    jobs = []
    #multiprocessing.set_start_method("fork", force=True)
    for procid in range(proc_cnt):
        params["cur_process"] = procid
        p = multiprocessing.Process(target=update_skus, args = (params,))
        jobs.append(p)
        p.start()
        time.sleep(1)
        proc_cnt += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def build_skus():
    '''
        Prep:
        db.createCollection("product_skus")
        db.product_skus.createIndex({"sku_nbr" : 1})
    '''
    collection = 'extra_card_full'
    tgt_coll = 'product_skus'
    cur_process = multiprocessing.current_process()
    procid = cur_process.name.replace("Process", "p")
    tstart_time = datetime.datetime.now()
    bstart_time = datetime.datetime.now()
    batch_size = 1000
    conn = client_connection("uri", settings)
    db = conn[settings["database"]]
    bb.logit(f'[{procid}] #--------------------- Starting ------------------------#')
    res = db[collection].distinct("sku_rank.SKU_NBR")
    bulk_docs = []
    cnt = 0
    totcnt = 0
    for k in res:
        doc = {}
        doc["sku_nbr"] = k
        doc["create_dttm"] = datetime.datetime.now()
        doc["version"] = "1.0"
        bulk_docs.append(doc)
        cnt += 1
        totcnt += 1
        if cnt == batch_size:
            db[tgt_coll].insert_many(bulk_docs)
            bb.logit(f"[{procid}] Processed: {totcnt} - in {timer(bstart_time)} secs")
            bstart_time = datetime.datetime.now()
            bulk_docs = []
            cnt = 0

    # get the leftovers
    if len(bulk_docs) > 0:
        db[tgt_coll].insert_many(bulk_docs)
        bb.logit(f'Final batch {len(bulk_docs)} to process')
    bb.logit("# -------------- COMPLETE ------------------- #")
    timer(tstart_time, False)

# --------------------------------------------------------------------------------------- #
#   Demo - Perfomance and Scaling, Indexes
#  BB - 10/19/23
# --------------------------------------------------------------------------------------- #
# Create synthetic Store Data BJB 10-16-23
def store_data():
    # Get unique list of store_ids from xtra_card
    # create store info
    collection = 'xtra_card_raw'
    tgt_coll = 'stores'
    cur_process = multiprocessing.current_process()
    procid = cur_process.name.replace("Process", "p")
    tstart_time = datetime.datetime.now()
    bstart_time = datetime.datetime.now()
    batch_size = 1000
    conn = client_connection("uri", settings)
    db = conn[settings["database"]]
    bb.logit(f'[{procid}] #--------------------- Starting ------------------------#')
    res = db[collection].distinct("HOME_STORE_NBR")
    bulk_docs = []
    cnt = 0
    totcnt = 0
    for k in res:
        doc = {}
        doc["store_nbr"] = k
        doc["create_dttm"] = datetime.datetime.now()
        doc["version"] = "1.0"
        doc["address1"] = fake.street_address()
        doc["address2"] = ""
        doc["city"] = fake.city()
        doc["state"] = fake.state()
        doc["zipcode"] = fake.zipcode()
        doc["geo"] = local_geo()
        bulk_docs.append(doc)
        cnt += 1
        totcnt += 1
        if cnt == batch_size:
            db[tgt_coll].insert_many(bulk_docs)
            bb.logit(f"[{procid}] Processed: {totcnt} - in {timer(bstart_time)} secs")
            bstart_time = datetime.datetime.now()
            bulk_docs = []
            cnt = 0

    # get the leftovers
    if len(bulk_docs) > 0:
        db[tgt_coll].insert_many(bulk_docs)
        bb.logit(f'Final batch {len(bulk_docs)} to process')
    bb.logit("# -------------- COMPLETE ------------------- #")
    timer(tstart_time, False)

# Create synthetic Recommendations BJB 10-16-23
def recommendations_data():
    # read settings and echo back
    bb.message_box("Recommendations Loader", "title")
    bb.logit(f'# Settings from: {settings_file}')
    # Spawn processes
    num_procs = 6
    jobs = []
    inc = 0
    idstart = 0
    idinc = 10000000
    # Cleanup:
    #idstart = 50000000
    #idinc = 2000000
    multiprocessing.set_start_method("fork", force=True)
    for item in range(num_procs):
        params = {"lowlim" : idstart + item * idinc, "inc" : idinc}
        p = multiprocessing.Process(target=recommendations_build, args = (item,params))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def recommendations_build(ipos, passed_args):
    # Get unique list of store_ids from xtra_card
    # create store info
    collection = 'xtra_card_raw'
    tgt_coll = 'recommendations'
    divs = 5
    inc = int(passed_args["inc"] / divs )
    lowlim = passed_args["lowlim"]
    cur_process = multiprocessing.current_process()
    procid = cur_process.name.replace("Process", "p")
    tstart_time = datetime.datetime.now()
    bstart_time = datetime.datetime.now()
    batch_size = 1200
    shelf = []
    conn = client_connection("uri", settings)
    db = conn[settings["database"]]
    file_log(f'# ------------- Starting: {lowlim}, inc: {inc} ---------------- #')
    bb.logit(f'[{procid}] #--------------------- Starting ({lowlim}) ------------------------#')
    shelf = build_shelf_items(db)
    shelf_size = len(shelf)
    id_low = lowlim
    id_high = lowlim + inc
    for ibatch in range(divs):
        bb.logit(f'[{procid}] # ---- Batch({ibatch}): {id_low} ---- #')
        file_log(f'Batch: from: {id_low} to: {id_high}')
        pipe = [
            {"$match" : {"$and" : [{"XTRA_CARD_NBR" : {"$gt" : id_low}},{"XTRA_CARD_NBR" : {"$lte" : id_high}}]}},
            {"$project" : {"XTRA_CARD_NBR" : 1}}
        ]
        res = db[collection].aggregate(pipe)
        # count to 10M by 2 millions in batches
        bulk_docs = []
        cnt = 0
        totcnt = 0
        for doc in res:
            icnt = 0
            for it in shelf:
                bulk_docs.append({
                    "_id" : f'SH-{doc["XTRA_CARD_NBR"]}-{icnt}',
                    "XTRA_CARD_NBR" : doc["XTRA_CARD_NBR"],
                    "SKU_NBR" : it["SKU_NBR"], 
                    "SKU_DSC" : it["SKU_DSC"]
                })
                icnt += 1
                cnt += 1
            totcnt += 1
            if cnt >= batch_size:
                print(f'# -------------------- {procid} - Bulk Docs ----------------------------')
                print(f'{doc["XTRA_CARD_NBR"]} - cnt: {cnt}, tot: {totcnt}')
                #if totcnt < 50:
                #    pprint.pprint(bulk_docs)
                db[tgt_coll].insert_many(bulk_docs)
                bb.logit(f"[{procid}] Processed: {totcnt} - in {timer(bstart_time)} secs")
                bstart_time = datetime.datetime.now()
                bulk_docs = []
                shelf = []
                shelf = build_shelf_items(db)
                cnt = 0
        # get the leftovers
        if len(bulk_docs) > 0:
            bb.logit(f'Final batch {len(bulk_docs)} to process')
            db[tgt_coll].insert_many(bulk_docs)
            
        bb.logit("# -------------- COMPLETE ------------------- #")
        timer(bstart_time, False)
        id_low += inc
        id_high = id_low + inc
    
    timer(tstart_time, False)

def build_shelf_items(db):
    pipe = [
        {"$sample" : {"size" : 60}}
    ]
    recs = db.product_skus.aggregate(pipe)
    res = []
    for it in recs:
        res.append({"SKU_NBR" : it["SKU_NBR"], "SKU_DSC" : it["SKU_DSC"]})
    return res


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
        print(f"{procid} - Bulk Load took {'{:.3f}'.format(execution_time)} seconds")
    return execution_time

def basictest():
    source_coll = 'xtra_card'
    alt_coll = 'xtra_card_mkt_sgmt'
    alt2_coll = 'xtra_card_sku_rank'
    key = "XTRA_CARD_NBR"
    cur_process = multiprocessing.current_process()
    procid = cur_process.name.replace("process", "p")
    tstart_time = datetime.datetime.now()
    conn = client_connection()
    db = conn[settings["database"]]
    cur = db[source_coll].find_one({}) #({"XTRA_CARD_NBR" : {"$lt" : 10000}}).limit(50)
    pprint.pprint(cur)
    #for k in cur:
    #    print(k["XTRA_CARD_NBR"])

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

def print_stats(start_t, msg = ""):
    end = datetime.datetime.now()
    elapsed = end - start_t
    secs = (elapsed.seconds) + elapsed.microseconds * .000001
    bb.logit(f'Elapsed: {"{:.3f}".format(secs)} - {msg}')

def query_mix():
    # mixed query
    conn = client_connection()
    db = conn[settings["database"]]
    start = datetime.datetime.now()
    limits = [0,1000000]
    if "limits" in ARGS:
        limits = ARGS["limits"].split(',')
    inc = 5000
    count = 2
    if "inc" in ARGS:
        inc = int(ARGS["inc"])
    if "count" in ARGS:
        count = int(ARGS["count"])
    min =int(limits[0])
    max = min + inc #int(limits[1])
    for k in range(count):
        start = datetime.datetime.now()
        res = db.xtra_card.aggregate(q_pipe(min,max))
        inum = 0
        for it in res:
            inum = it["num_records"]
        print_stats(start, f'max={max}, docs={inum}')
        max += inc
        min += inc

def find_mix():
    # mixed query
    conn = client_connection()
    db = conn[settings["database"]]
    start = datetime.datetime.now()
    bb.message_box("Performing find mix")
    q_find(db["xtra_card_full"])

def q_find(coll):
    card_ids = [120702671,56382424,129324067,393666906,302201229,63036879,96910041,122701609,157212897,444088055,922638,32920017,396113799,477765478,470238382,1472441,478653700,305760436,129218422,508015378,392387025,13275126,168168817,415788351,498557897,86958999,93752174,211511893,119900651,435422817,302728693,489566082,33854685,216098120,46351162,43697406,502330516,100004740,152734467,488069720,52546437,454190859,509518802,58722957,230316401,41629451,36970911,466305554,36086319,428234452,513888050,495327085,95372017,461352724,388840335,172528559,410519384,59355645,257707349,125608003,317981502,305490606,471842135,481464459,54104661,390757679,120030189,397122881,317714954,484045745,343698675,27610331,296038748,100028469,472564041,159863658,37214494,191105762,429271628,419021463,123762459,31432850,491877218,94208476,339017440,74275982,55295226,330207587,188945370,40750873,133522635,37455824]
    if "agg" in ARGS:
        bb.logit("Using lookup aggregation")
    else:
        bb.logit("Using simple find")
    for it in card_ids:
        start = datetime.datetime.now()
        if "agg" in ARGS:
            res = coll.aggregate(q_pipe_lookup(it))
        elif "coupon" in ARGS:
            res = coll.aggregate(q_pipe_lookup_coupon(it))
        else:
            res = coll.find_one({"XTRA_CARD_NBR" : it}) #,{"_id": 0,"XTRA_CARD_NBR":1}
        print(list(res)[0])
        print_stats(start, f" - id: {it}")
    
def q_pipe(min,max):
    pipe = [
        {'$match': {
                '$and': [{'XTRA_CARD_NBR': {'$gt': min}}, {'XTRA_CARD_NBR': {'$lte': max}}]
        }}, 
        #{'$skip': 0}, 
        #{'$limit': 100}, 
        {'$lookup': {
                'from': 'xtra_card_sku_rank', 
                'localField': 'XTRA_CARD_NBR', 
                'foreignField': 'XTRA_CARD_NBR', 
                'pipeline': [
                    {'$project': {'_id': 0, 'XTRA_CARD_NBR': 0}}
                ], 
                'as': 'sku_rank'
            }
        }, 
        {'$lookup': {
                'from': 'xtra_card_mkt_sgmt', 
                'localField': 'XTRA_CARD_NBR', 
                'foreignField': 'XTRA_CARD_NBR', 
                'pipeline': [
                    {'$project': {'_id': 0, 'XTRA_CARD_NBR': 0}}
                ], 
                'as': 'mkt_sgmt'
            }
        }, 
        {'$unwind': {'path': '$mkt_sgmt'}
        }, 
        {'$count': 'num_records'}
    ]
    return(pipe)

def q_pipe_lookup(card_id):
    pipe = [
        {'$match': {
                'XTRA_CARD_NBR': card_id}
        }, 
        {'$lookup': {
                'from': 'xtra_card_sku_rank', 
                'localField': 'XTRA_CARD_NBR', 
                'foreignField': 'XTRA_CARD_NBR', 
                'pipeline': [
                    {'$project': {'_id': 0, 'XTRA_CARD_NBR': 0}}
                ], 
                'as': 'sku_rank'
            }
        }, 
        {'$lookup': {
                'from': 'xtra_card_mkt_sgmt', 
                'localField': 'XTRA_CARD_NBR', 
                'foreignField': 'XTRA_CARD_NBR', 
                'pipeline': [
                    {'$project': {'_id': 0, 'XTRA_CARD_NBR': 0}}
                ], 
                'as': 'mkt_sgmt'
            }
        }, 
        {'$unwind': {'path': '$mkt_sgmt'}
        } 
        #{'$count': 'num_records'}
    ]
    return(pipe)

def q_pipe_lookup_coupon(card_id):
    pipe = [
        {'$match': {
                'XTRA_CARD_NBR': card_id}
        }, 
        {'$lookup': {
                'from': 'xtra_card_mfr_coupon_rank', 
                'localField': 'XTRA_CARD_NBR', 
                'foreignField': 'XTRA_CARD_NBR', 
                'pipeline': [
                    {'$project': {'_id': 0, 'XTRA_CARD_NBR': 0}}
                ], 
                'as': 'coupon_rank'
            }
        }, 
        {'$lookup': {
                'from': 'xtra_card_mkt_sgmt', 
                'localField': 'XTRA_CARD_NBR', 
                'foreignField': 'XTRA_CARD_NBR', 
                'pipeline': [
                    {'$project': {'_id': 0, 'XTRA_CARD_NBR': 0}}
                ], 
                'as': 'mkt_sgmt'
            }
        }, 
        {'$unwind': {'path': '$mkt_sgmt'}
        } 
        #{'$count': 'num_records'}
    ]
    return(pipe)

#----------------------------------------------------------------------#
#   Utility Routines
#----------------------------------------------------------------------#
def local_geo():
    coords = fake.local_latlng('US', True)
    return coords

def client_connection(type = "uri", details = {}):
    global settings
    if not 'settings' in locals() and not 'settings' in globals():
        settings = details
    #pprint.pprint(settings)
    mdb_conn = settings[type]
    username = settings["username"]
    password = settings["password"]
    if "username" in details:
        username = details["username"]
        password = details["password"]
    if "secret" in password:
        password = os.environ.get("_PWD_")
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
    settings_file = "recommendations_settings.json"
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    g_lock = "" #Lock()
    g_logger = file_log("init", g_lock)
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
    elif ARGS["action"] == "load_csv_data":
        build_csvs()
    elif ARGS["action"] == "load_skus":
        worker_sku_update()
    elif ARGS["action"] == "customer_load":
        worker_customer_load()
    elif ARGS["action"] == "recommendations_load":
        worker_load_recommendations()
    elif ARGS["action"] == "poc_load":
        build_recommendation_data()
    elif ARGS["action"] == "merge":
        xtracard_merger_all()
    elif ARGS["action"] == "query_mix":
        query_mix()
    elif ARGS["action"] == "find_mix":
        find_mix()
    elif ARGS["action"] == "store_data":
        store_data()
    elif ARGS["action"] == "recs_data":
        recommendations_data()
    elif ARGS["action"] == "test":
        basictest()
    elif ARGS["action"] == "mergemulti":
        load_enriched_data()
    elif ARGS["action"] == "file_update":
        update_file()
    elif ARGS["action"] == "file_restart":
        file_restart()
    elif ARGS["action"] == "export":
        export_filer()
    elif ARGS["action"] == "build_skus":
        build_skus()
    elif ARGS["action"] == "update_skus":
        update_sku_master()
    elif ARGS["action"] == "update_bought":
        update_bought()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()

