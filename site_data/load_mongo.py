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
base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(base_dir))
from bbutil import Util
from id_generator import Id_generator
import process_csv_model as csvmod
import datetime
import pymongo
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
from pymongo import UpdateMany
from faker import Faker

fake = Faker()
settings_file = "site_settings.json"

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
    #  Reads EMR sample file and finds values
    cur_process = multiprocessing.current_process()
    pid = cur_process.pid
    conn = client_connection()
    bb.message_box(f"[{pid}] Worker Data", "title")
    settings = bb.read_json(settings_file)
    batch_size = settings["batch_size"]
    batches = settings["batches"]
    bb.logit('Current process is %s %s' % (cur_process.name, pid))
    start_time = datetime.datetime.now()
    collection = settings["mongodb"]["collection"]
    db = conn[settings["mongodb"]["database"]]
    #IDGEN = Id_generator({"seed" : base_counter, "size" : count})
    bulk_docs = []
    ts_start = datetime.datetime.now()
    job_size = batches * batch_size
    if "size" in ARGS:
        job_size = int(ARGS["size"])
    cur_time = ts_start
    cnt = 0
    tot = 0
    if "template" in args:
        template = args["template"]
        master_table = master_from_file(template)
        job_info = {master_table : {"path" : template, "size" : job_size, "id_prefix" : f'{master_table[0].upper()}-'}}
    else:
        job_info = settings["data"]
    # Loop through collection files
    for domain in job_info:
        details = job_info[domain]
        prefix = details["id_prefix"]
        count = details["size"]
        template_file = details["path"]
        design = csvmod.doc_from_template(template_file, domain)
        base_counter = settings["base_counter"] + count * ipos
        IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefix})
        bb.message_box(f'[{pid}] {domain} - base: {base_counter}', "title")
        tot = 0
        tot_ops = 0
        batches = int(count/batch_size)
        if batches == 0:
            batches = 1
        for k in range(1): #range(batches):
            #bb.logit(f"[{pid}] - {domain} Loading batch: {k} - size: {batch_size}")
            bulk_docs = build_batch_from_template(domain, {"design": design, "connection" : conn, "template" : template_file, "batch" : k, "id_prefix" : prefix, "base_count" : base_counter, "size" : count})
            #print(bulk_docs)
            db[domain].insert_many(bulk_docs)
            tot_ops += 1
            if tot_ops > 10000:
                conn.close()
                tot_ops = 0
                conn = client_connection()
                db = conn[settings["mongodb"]["database"]]
            tot += len(bulk_docs)
            bulk_docs = []
            cnt = 0
            bb.logit(f"[{pid}] - {domain} Loading batch: {k} - size: {batch_size}, Total:{tot}\nIDGEN - ValueHist: {IDGEN.value_history}")
        ensure_indexes(template_file, domain, db)
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    conn.close()
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def build_batch_from_template(cur_coll, details = {}):
    batch_size = settings["batch_size"]
    if "size" in details and details["size"] < batch_size:
        batch_size = details["size"]
    design = details["design"]
    sub_size = 5
    cnt = 0
    records = []
    #print("# --------------------------- Initial Doc -------------------------------- #")
    #pprint.pprint(design)
    #print("# --------------------------- End -------------------------------- #")
    result = []
    for J in range(batch_size): # iterate through the bulk insert count
        # A dictionary that will provide consistent, random list lengths
        counts = random.randint(1, sub_size) #defaultdict(lambda: random.randint(1, 5))
        data = {}
        cdesign = copy.deepcopy(design)
        data = render_design(cdesign, counts)
        data["doc_version"] = settings["version"]
        cnt += 1
        records.append(data)
    bb.logit(f'{batch_size} {cur_coll} batch complete')
    return(records)

def render_design(design, count):
    # Takes template and evals the gnerators
    #pprint.pprint(design)
    for key, val in design.items():
        if isinstance(val, dict):
            render_design(val,count)
        elif isinstance(val, list):
            render_design(val[0],count)
        else:
            try:
                design[key] = eval(val)
            except Exception as e:
                print(f'ERROR: eval: {val}')
    return design

def master_from_file(file_name):
    return file_name.split("/")[-1].split(".")[0]

def ID(key):
    id_map[key] += 1
    return key + str(id_map[key]+base_counter)

def local_geo():
    coords = fake.local_latlng('US', True)
    return coords

def ensure_indexes(template, domain, db_conn):
    i_fields = csvmod.indexed_fields_from_template(template, domain)
    cur = db_conn[domain].index_information()
    keys = cur.keys()
    for fld in i_fields:
        if fld not in keys:
            bb.logit(f"Creating index: {fld}")
            db_conn[domain].create_index([(fld, pymongo.ASCENDING)])

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
    quotes = ['P-2016127','P-2049723','P-2049772','P-2049770','P-2049884','P-2049893','P-2049834','P-2049768','P-2049993','P-2047765','P-2049991','P-2049862','P-2049977','P-2018808','P-2049965','P-2049818','P-2049932','P-2049852','P-2049788','P-2049990''P-2001490','P-2049718','P-2049989','P-2049940','P-2049927','P-2049959','P-2049967','P-2049816','P-2049954','P-2049937','P-2049975','P-2049972','P-2049846','P-2049988','P-2049822','P-2049725','P-2049930','P-2049985','P-2003470','P-2049905''P-2049693','P-2049982','P-2049912','P-2049890','P-2049882','P-2049800','P-2049885','P-2049726','P-2015704','P-2049872','P-2049735','P-2038890','P-2049909','P-2001723','P-2049899','P-2049850','P-2049806','P-2049804','P-2049767','P-2014746''P-2036198','P-2049861','P-2049833','P-2001972','P-2049762','P-2049970','P-2006118','P-2049813','P-2045490','P-2012698','P-2049920','P-2013444','P-2049941','P-2049949','P-2049964','P-2049979','P-2049943','P-2044278','P-2049708','P-2049986''P-2049875','P-2006814','P-2049894','P-2049874','P-2049925','P-2049815','P-2049836','P-2049849','P-2049935','P-2049841','P-2043169','P-2049791','P-2049961','P-2049971','P-2010675','P-2049948','P-2031167','P-2049945','P-2049790','P-2049942']

    cur_process = multiprocessing.current_process()
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    bb.logit("Performing 5000 queries")
    conn = client_connection()
    db = conn[settings["mongodb"]["database"]]
    coll = settings["mongodb"]["collection"]
    num = len(quotes)
    cnt = 0
    for k in range(int(1000/num)):
        for curid in quotes:
            start = datetime.datetime.now()
            output = db.claim.find_one({"quote_id" : curid })
            if cnt % 100 == 0:
                bb.timer(start, 100)
                #bb.logit(f"{cur_process.name} - Query: Disease: {term} - Elapsed: {format(secs,'f')} recs: {output} - cnt: {cnt}")
            cnt += 1
            #time.sleep(.5)
    conn.close()

def time_query():
    cur_process = multiprocessing.current_process()
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    bb.logit("Performing 100 queries")
    conn = client_connection()
    query = json.loads(ARGS["query"])
    db = conn[settings["mongodb"]["database"]]
    cnt = 0
    for k in range(100):
        start = datetime.datetime.now()
        output = db.claim.find(query)
        inc = 0
        for it in output:
            inc += 1
        bb.timer(start, inc)
        cnt += 1
    conn.close()

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
    IDGEN = Id_generator({"seed" : base_counter})
    id_map = defaultdict(int)
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
        synth_data_load()
    elif ARGS["action"] == "query":
        run_query()
    elif ARGS["action"] == "timer":
        time_query()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()
