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
 #  MyPBM - ClaimCache

Intermediary Cache from DTShare (cdc from DataGeneral to Warehouse in teradata)
#  BJB 7/13/22

    python3 claimcache_pbm.py action=load_csv

# Startup Env:
    Atlas M10BasicAgain

#  Use Case

#  Methodology
    Simulate the feed from DTshare - simple 30attr document represents an update on MF
    Store raw document - 2M/hr
    Update "current" state of claim
    create update documents
    create master claim docs
    create trigger and function to update
        
'''
settings_file = "claimcache_pbm_settings.json"

def load_claim_updates():
    # read settings and echo back
    bb.message_box("ClaimCache Loader", "title")
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
    use_csv = False
    if "csv" in ARGS:
        csv = True
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    #file_log(f'New process {cur_process.name}')
    start_time = datetime.datetime.now()
    if ipos == 0 and not tester:
        worker_history_load(feed)
    elif csv:
        worker_history_load_csv(feed)
    else:
        worker_history_tester(feed)
    #worker_claim_load(pgcon,tables)
    #worker_rx_load(pgcon,tables)
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    #file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def worker_history_load(num_iterations):
    #  Send a copy of the claim with one or two field changes
    #  Add an updateDate and sequencenumber, updateName = TCD-1
    cur_process = multiprocessing.current_process()
    collection = settings["alt_collection"]
    claimcoll = settings["collection"]
    prefix = "CLU"
    interval = 30
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
        bb.message_box("History Feed Simulation")
        bb.logit(f'Iter: {k} of {num_iterations}, {sample_size} per batch')
        claim_cur = db[claimcoll].aggregate(pipe)
        IDGEN.set({"seed" : base_counter, "size" : num_records, "prefix" : prefix})
        base_id = int(IDGEN.get(prefix, sample_size).replace(prefix,""))
        bulk_docs = []
        bulk_updates = []
        icnt = 0
        for row in claim_cur:
            new_id = f'{prefix}{base_id + icnt}'
            if icnt != 0 and icnt % batch_size == 0:
                db[collection].insert_many(bulk_docs)
                bulk_writer(db[claimcoll],bulk_updates)
                bb.logit(f'Adding {batch_size} total: {icnt}')
                bulk_docs = []
                bulk_updates = []
                bb.logit(f'Delay, waiting: {interval} seconds')
                time.sleep(interval)
            bulk_docs.append(claim_doc(new_id, row))
            bulk_updates.append(UpdateOne({"_id" : row["_id"]},{"$inc" : {"sequence_id" : 1}}))
            icnt += 1
        # get the leftovers
        bulk_writer(db[claimcoll],bulk_updates)
        db[collection].insert_many(bulk_docs)
    bb.logit("#-------- COMPLETE -------------#")
    conn.close()

def worker_history_load_csv(export_path):
    #  Send a copy of the claim with one or two field changes
    #  Add an updateDate and sequencenumber, updateName = TCD-1
    cur_process = multiprocessing.current_process()
    collection = settings["alt_collection"]
    claimcoll = settings["collection"]
    prefix = "CLU"
    interval = 30
    conn = client_connection()
    #export_path = "updatecsv/tcd_update.csv"
    db = conn[settings["database"]]
    base_counter = settings["base_counter"]
    batch_size = settings["batch_size"]
    num_records = settings["num_records"]
    sampler = settings["num_csvupdates"]
    sample_size = int(num_records * sampler)
    if "size" in ARGS:
        sample_size = int(ARGS["size"])
    pipe = [{"$sample" : {"size" : sample_size}}] # Do 10% at a time
    claim_cur = db[claimcoll].aggregate(pipe)
    with open(export_path,'w') as fil:
        bb.message_box("History Feed Simulation")
        #bb.logit(f'Iter: {k} of {num_iterations}, {sample_size} per batch')
        IDGEN.set({"seed" : base_counter, "size" : num_records, "prefix" : prefix})
        base_id = int(IDGEN.get(prefix, sample_size).replace(prefix,""))
        writer = csv.writer(fil)
        bulk_updates = []
        icnt = 0
        for row in claim_cur:
            new_id = f'{prefix}{base_id + icnt}'
            newdoc = claim_doc(new_id, row)
            if icnt == 0:
                writer.writerow(newdoc.keys())
            elif icnt % batch_size == 0:
                bulk_writer(db[claimcoll],bulk_updates)
                bb.logit(f'Adding {batch_size} total: {icnt}')
                bulk_updates = []
                #bb.logit(f'Delay, waiting: {interval} seconds')
                #time.sleep(interval)
            writer.writerow(newdoc.values())
            bulk_updates.append(UpdateOne({"_id" : row["_id"]},{"$inc" : {"sequence_id" : 1}}))
            icnt += 1
        # get the leftovers
        bulk_writer(db[claimcoll],bulk_updates)
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

def claim_doc(idval, curclaim):     
    age = random.randint(28,84)
    year = 2020 - age
    month = random.randint(1,12)
    day = random.randint(1,28)
    name = fake.name()
    status_codes = ["500","600","700","701","720","721","727","770","800"]
    status_code_names = ["submitted","adjudicating","preliminary-approval","information-requested","type_change","rejected","deductible-not-met","out-of-network","approved"]
    doc = {}
    doc['claim_history_id'] = idval
    doc["claim_id"] = curclaim["claim_id"]
    doc["member_id"] = curclaim["member_id"]
    doc['updated_at'] = datetime.datetime.now()
    doc['sequence_id'] = curclaim["sequence_id"] + 1
    k = random.randint(0,8)
    doc['claimStatus'] = status_codes[k]
    doc['claimStatusText'] = status_code_names[k]
    doc['claimStatusDate'] =  datetime.datetime.now()
    doc['claimType'] = curclaim["claimType"]
    doc['notes'] = fake.sentence()
    doc["plan_id"] = curclaim["plan_id"]
    doc["planSubscriber_id"] = curclaim["planSubscriber_id"]
    doc["principalDiagnosis"] = curclaim["principalDiagnosis"]
    doc["priorAuthorization"] = random.choice([True,False]) #curclaim["priorAuthorization"]
    doc["placeOfService"] = curclaim["placeOfService"]
    doc["serviceEndDate"] = curclaim["serviceEndDate"]
    doc["serviceFacility_id"] = curclaim["serviceFacility_id"]
    doc["prescriber_id"] = curclaim["prescriber_id"]
    doc["version"] = "1.0"
    #doc["notes"].append({"claimNoteText" : fake.sentence(), "updatedAt" : datetime.datetime.now()})
    return(doc)

def get_claim_ids(coll):
    MASTER_CLAIMS = []
    cur = coll.find({},{"_id" : 0 ,"claim_id" : 1})
    for it in cur:
        MASTER_CLAIMS.append(it["claim_id"])

#----------------------------------------------------------------------#
#   Borrow claim load from relational_patterns
#----------------------------------------------------------------------#
def csv_data_load():
    multiprocessing.set_start_method("fork", force=True)
    bb.message_box("Loading Data", "title")
    bb.logit(f'# Settings from: {settings_file}')
    passed_args = {"ddl_action" : "info"}
    if "template" in ARGS:
        template = ARGS["template"]
        passed_args["template" : template]
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
        p = multiprocessing.Process(target=csv_worker_load, args = (item, passed_args))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def csv_worker_load(ipos, args):
    #  Process thread worker
    cur_process = multiprocessing.current_process()
    pid = cur_process.pid
    conn = client_connection()
    bb.message_box(f"[{pid}] Worker Data", "title")
    settings = bb.read_json(settings_file)
    batch_size = settings["batch_size"]
    batches = settings["batches"]
    bb.logit('Current process is %s %s' % (cur_process.name, pid))
    start_time = datetime.datetime.now()
    collection = settings["collection"]
    db = conn[settings["database"]]
    bulk_docs = []
    ts_start = datetime.datetime.now()
    cur_time = ts_start
    cnt = 0
    tot = 0
    job_info = settings["data"]
    # Loop through collection files
    for domain in job_info:
        details = job_info[domain]
        prefix = details["id_prefix"]
        count = details["size"]
        template_file = details["path"]
        base_counter = settings["base_counter"] + count * ipos
        IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefix})
        bb.message_box(f'[{pid}] {domain} - base: {base_counter}', "title")
        tot = 0
        batches = int(details["size"]/batch_size)
        for k in range(batches):
            #bb.logit(f"[{pid}] - {domain} Loading batch: {k} - size: {batch_size}")
            bulk_docs = build_batch_from_template(domain, {"connection" : conn, "template" : template_file, "batch" : k, "id_prefix" : prefix, "base_count" : base_counter})
            #print(bulk_docs)
            db[domain].insert_many(bulk_docs)
            tot += len(bulk_docs)
            bulk_docs = []
            cnt = 0
            bb.logit(f"[{pid}] - {domain} Loading batch: {k} - size: {batch_size}, Total:{tot}\nIDGEN - ValueHist: {IDGEN.value_history}")
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    conn.close()
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def build_batch_from_template(cur_coll, details = {}):
    template_file = details["template"]
    batch_size = settings["batch_size"]
    cnt = 0
    records = []
    merger = Merger([
        (dict, "merge"),
        (list, zipmerge)
    ], [ "override" ], [ "override" ])
    for J in range(0, batch_size): # iterate through the bulk insert count
        # A dictionary that will provide consistent, random list lengths
        counts = defaultdict(lambda: random.randint(1, 5))
        data = {}
        with open(template_file) as csvfile:
            propreader = csv.reader(itertools.islice(csvfile, 1, None))
            for row in propreader:
                path = row[0].split('.')
                partial = procpath(path, counts, row[3]) # Note, later version of files may not include required field
                data = merger.merge(data, partial)
        data = list(data.values())[0]
        cnt += 1
        records.append(data)
    #bb.logit(f'{batch_size} {cur_coll} batch complete')
    return(records)

#----------------------------------------------------------------------#
#   CSV Loader Routines
#----------------------------------------------------------------------#
stripProp = lambda str: re.sub(r'\s+', '', (str.strip('()')))

def ser(o):
    """Customize serialization of types that are not JSON native"""
    if isinstance(o, datetime.datetime.date):
        return str(o)

def procpath(path, counts, generator):
    """Recursively walk a path, generating a partial tree with just this path's random contents"""
    stripped = stripProp(path[0])
    if len(path) == 1:
        # Base case. Generate a random value by running the Python expression in the text file
        return { stripped: eval(generator) }
    elif path[0].endswith('()'):
        # Lists are slightly more complex. We generate a list of the length specified in the
        # counts map. Note that what we pass recursively is _the exact same path_, but we strip
        # off the ()s, which will cause us to hit the `else` block below on recursion.
        return {
            stripped: [ procpath([ path[0].strip('()') ] + path[1:], counts, generator)[stripped] for X in range(0, counts[stripped]) ]
        }
    else:
        # Return a nested page, of the specified type, populated recursively.
        return {stripped: procpath(path[1:], counts, generator)}

def ID(key):
    id_map[key] += 1
    return key + str(id_map[key]+base_counter)

def zipmerge(the_merger, path, base, nxt):
    """Strategy for deepmerge that will zip merge two lists. Assumes lists of equal length."""
    return [ the_merger.merge(base[i], nxt[i]) for i in range(0, len(base)) ]

def local_geo():
    coords = fake.local_latlng('US', True)
    return coords

#----------------------------------------------------------------------#
#   File Monitor and CSV Import
#----------------------------------------------------------------------#
def file_monitor():
    main_process = multiprocessing.current_process()
    bb.message_box("CSV Importer", "title")
    path = f'{CUR_PATH}/{settings["import_path"]}'
    if "path" in ARGS:
        path = ARGS["path"]
    bb.logit(f'Watching {path}')
    bb.logit('# Main process is %s %s' % (main_process.name, main_process.pid))
    icnt = 0
    keep_checking = True
    while keep_checking:
        file_walk(path, icnt)
        time.sleep(10)
        icnt += 1

def file_walk(path, iter):
    #we shall store all the file names in this list
    collection = settings["collection"]
    bulk_docs = []
    testy = []
    path_list = []
    batch_size = settings["batch_size"]
    bb.logit(f'Executing: {iter}')
    conn = client_connection()
    db = conn[settings["database"]]
    cnt = 0
    tot = 0
    target_dir = os.path.join(CUR_PATH,"processedcsv")
    for root, dirs, files in os.walk(path):
        bulk_docs = []
        cnt = 0
        bb.logit("#--------------------------------------------------#")
        bb.logit(f'# {root}')
        dir_doc = file_info(root, "dir")
        #db[collection].insert_one(dir_doc)
        for fil in files:
            cur_file = os.path.join(root,fil)
            new_doc = (file_info(cur_file))
            bulk_docs.append(new_doc)
            bb.logit(cur_file)
            cnt += 1
            worker_history_load_csv(cur_file)
            bb.logit(f"Processed: {fil}")
            shutil.move(cur_file, target_dir)
 

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

def claim_detail():
    #  Take an array of claims, give the timeline of changes
    #  Add an updateDate and sequencenumber, updateName = TCD-1
    cur_process = multiprocessing.current_process()
    claimcoll = "claims"
    claim_ids = []
    if "claim_ids" in ARGS:
        inp = ARGS["claim_ids"].split(",")
        for k in inp:
            claim_ids.append(k.strip())
    else:
        print("Send claim_ids=<id1,id2,id3>")
        sys.exit(1)    # Spawn processes
    conn = client_connection()
    db = conn[settings["database"]]
    pipe = [
        {
            '$match': {
                'claim_id': {"$in" : claim_ids}
            }
        }, 
        {   
            '$sort' : {"claim_id" : 1, "updated_at" : 1}
        }
    ]
    bb.message_box("Claim Report")
    bb.logit(f'ClaimIDs: {", ".join(claim_ids)}')
    claim_cur = db[claimcoll].aggregate(pipe)
    col_sizes = [20,12,20]
    icnt = 0
    last_id = "xxxxxx"
    for row in claim_cur:
        if last_id != row["claim_id"]:
            last_id = row["claim_id"]
            bb.logit(f'#===  ClaimID: {last_id}  ===#')
        pprint.pprint(row)

def history_report():
    #  Take an array of claims, give the timeline of changes
    #  Add an updateDate and sequencenumber, updateName = TCD-1
    cur_process = multiprocessing.current_process()
    claimcoll = "claim_history"
    claim_ids = []
    if "claim_ids" in ARGS:
        inp = ARGS["claim_ids"].split(",")
        for k in inp:
            claim_ids.append(k.strip())
    else:
        print("Send claim_ids=<id1,id2,id3>")
        sys.exit(1)    # Spawn processes
    conn = client_connection()
    db = conn[settings["database"]]
    pipe = [
        {
            '$match': {
                'claim_id': {"$in" : claim_ids}
            }
        }, 
        {   
            '$sort' : {"claim_id" : 1, "updated_at" : 1}
        },
        {
            '$project': {
                'claim_id' : 1,
                'updated_at': 1, 
                'claimStatus': 1, 
                'claimStatusText': 1, 
                'claimType': 1
            }
        }
    ]
    bb.message_box("Claim History Report")
    bb.logit(f'ClaimIDs: {", ".join(claim_ids)}')
    claim_cur = db[claimcoll].aggregate(pipe)
    col_sizes = [20,12,30]
    icnt = 0
    bb.table("title", ["Updated","Type","Status"], col_sizes, False)
    last_id = "xxxxxx"
    for row in claim_cur:
        if last_id != row["claim_id"]:
            last_id = row["claim_id"]
            bb.table("border", ["Updated","Type","Status"], col_sizes, False)
            print(f'#---  ClaimID: {last_id}  ---#')
        bb.table("data",[row["updated_at"].strftime('%Y-%m-%dT%H:%M:%S'),row["claimType"],f'{row["claimStatus"]}-{row["claimStatusText"]}'], col_sizes, False)
    bb.table("border", ["Updated","Type","Status"], col_sizes, False)

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
    elif ARGS["action"] == "load_claims":
        csv_data_load()
    elif ARGS["action"] == "load_claim_updates":
        # python3 claimcache_pbm.py action=load_claim_updates [feed=true | test=true | size=13]
        load_claim_updates()
    elif ARGS["action"] == "customer_load":
        worker_customer_load()
    elif ARGS["action"] == "monitor":
        file_monitor()
    elif ARGS["action"] == "recommendations_load":
        worker_load_recommendations()
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

#---- Queries --------#
Find Claim
    python3 claimcache_pbm.py action=reports report=claim_details claim_ids=C-1008455,C-1006512
Find ClaimHistory
    python3 claimcache_pbm.py action=reports report=claim_history claim_ids=C-1000787,C-1000216, C-1000277
python3 claimcache_pbm.py action=load_claim_updates

Attribute changes
"C-1006512" has 5 updates
"C-1008455", "C-1006512"


db[collection].aggregate([
  {"$group":
     {_id:{provider:"$provider", timestamp:"$timestamp"}, 
      items:{$addToSet:{name:"$name",value:"$value"}}}
  }, 
  {$project:
     {tmp:{$arrayToObject: 
       {$zip:{inputs:["$items.name", "$items.value"]}}}}
  }, 
  {$addFields:
     {"tmp.provider":"$_id.provider", 
      "tmp.timestamp":"$_id.timestamp"}
  }, 
  {$replaceRoot:{newRoot:"$tmp"}
  }
])


[
    {
        '$group': {
            '_id': '$claim_id', 
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


#-------------------------------------#

            Upd-1       Upd-2       Upd-3       Upd-4
claimStatus 718         747         803         899
statusText  initiate    in-process  rejected    resolved
claimType   pharm       pharm       pharm       pharm
updated_at  11/7        11/9        11/14        12/5

claimStatus
718
747
803
899

statusText
initiate
in-process
rejected
resolved


[
    {
        '$match': {
            'claim_id': 'C-1006512'
        }
    }, {
        '$group': {
            '_id': '$claim_id', 
            'aclaimStatus': {
                '$addToSet': {
                    'k': 'claimStatus', 
                    'v': '$claimStatus'
                }
            }, 
            'aclaimStatusText': {
                '$addToSet': {
                    'k': 'claimStatusText', 
                    'v': '$claimStatusText'
                }
            }, 
            'aSequence_id': {
                '$addToSet': {
                    'k': 'sequence_id', 
                    'v': '$sequence_id'
                }
            }, 
            'aclaimType': {
                '$addToSet': {
                    'k': 'claimType', 
                    'v': '$claimType'
                }
            }
        }
    }, {
        '$project': {
            'xpose': {
                '$arrayToObject': '$aclaimStatus'
            }
        }
    }
]

#------------------------------------------------#
[
    {
        '$match': {
            'claim_id': 'C-1006512'
        }
    }, {
        '$group': {
            '_id': '$claim_id',
            'ClaimStatus': {
                '$addToSet': '$claimStatus'
            },
            'ClaimStatusText': {
                '$addToSet': '$claimStatusText'
            },
            'Sequence_id': {
                '$addToSet': '$sequence_id'
            },
            'ClaimType': {
                '$addToSet': '$claimType'
            },
            'Updated': {
                '$addToSet': '$updated_at'
            }
        }
    },
    {"$project : {
        Updated : 1, ClaimStatus : 1, ClaimStatusText : 1, ClaimType: 1
    }}
]

#-----------------------------------------------------------#
[
    {
        '$match': {
            'claim_id': 'C-1006512'
        }
    }, {
        '$project': {
            'updated_at': 1, 
            'claimStatus': 1, 
            'claimStatusText': 1, 
            'claimType': 1
        }
    }
]
'''