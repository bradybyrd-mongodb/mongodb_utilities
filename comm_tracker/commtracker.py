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
#from deepmerge import Merger
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
#from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
base_dir = os.path.dirname(os.path.abspath(__file__))
# apppend parent folder to path
sys.path.append(os.path.dirname(base_dir))
sys.path.append(os.path.join(base_dir, "templates"))
from bbutil import Util
from id_generator import Id_generator
from mongo_loader import DbLoader
from message_loader import MessageLoader
from perf_queries import PerfQueries


fake = Faker()
letters = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
providers = ["cigna","aetna","anthem","bscbsma","kaiser"]

'''
 #  CommTracker
#  BJB 8/18/22
Communications Cache from Hadoop UDF

    python3 commtracker.py action=load_csv

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
settings_file = "commtracker_settings.json"

def load_messages():
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
        worker_message_generate(feed)
    #worker_claim_load(pgcon,tables)
    #worker_rx_load(pgcon,tables)
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    #file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

# --------------------------------------------------------- #
#  Pub Sub Publish
# --------------------------------------------------------- #
def message_publisher(stream):
    # read settings and echo back
    bb.message_box("Pub/Sub Message Subscriiption", "title")
    bb.logit(f'# Settings from: {settings_file}')
    # Spawn processes
    num_procs = settings["process_count"]
    topics = list(settings["topics"].keys())
    if "topics" in ARGS:
        topics = ARGS["topics"].replace(" ","").split(",")
    jobs = []
    inc = 0
    multiprocessing.set_start_method("fork", force=True)
    for topic_id in range(len(topics)):
        for item in range(num_procs):
            p = multiprocessing.Process(
                target=worker_message_generate, args=(item, stream, topics[topic_id]))
            jobs.append(p)
            p.start()
            time.sleep(1)
            inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()


def worker_message_generate(proc_num, stream, topic = "direct"):
    '''
        generate messages - publish to pub/sub
        accumulate x-million
        start consumer
            read from pub/sub
            push to mongoDB (collection per topic)

        single-instance now
        gke - scale up
    
    '''
    #  Send a copy of the claim with one or two field changes
    #  Add an updateDate and sequencenumber, updateName = TCD-1
    cur_process = multiprocessing.current_process()
    cprocess = cur_process.name.replace("Process","p") # Process-7 p-7
    prefix = "COMT"
    feed = False
    if stream == "direct":
        loader = None
    else:
        loader = MessageLoader(topic, {"settings": settings})
    global interval_vars
    interval_vars = {}
    base_counter = settings["base_counter"]
    batch_size = settings["batch_size"]
    num_records = settings["num_records"]
    interval = settings["sample_time"]
    base_counter = base_counter + num_records * proc_num
    summary_template = settings["summary_template"]
    num_iterations = int(num_records/batch_size)
    IDGEN.set({"seed" : base_counter, "size" : num_records, "prefix" : prefix})
    if "feed" in ARGS:
        sample_size = 10
        num_iterations = int(ARGS["feed"])
        interval = 5
        feed = True
    if "size" in ARGS:
        sample_size = int(ARGS["size"])
    bb.message_box("Comm Feed Simulation")
    if stream == "kafka":
        loader.add = loader.add_kafka
        bb.logit(f"[{cur_process.name}] Pubishing Kafka Messages - {num_records} to do")
    elif stream == "pubsub":
        loader.add = loader.add_pubsub
    elif stream == "direct":
        meth = "uri"
        if "cosmos" in ARGS:
            meth = "cosmos"
        conn = client_connection(meth)
    icnt = 0
    bulk_docs = []
    start_time = datetime.datetime.now()
    istart_time = datetime.datetime.now()
    base_id = int(IDGEN.get(prefix, num_records).replace(prefix,""))
    for k in range(num_iterations):
        #bb.logit(f'Iter: {k} of {num_iterations}, {batch_size} per batch - total: {icnt}')
        for row in range(batch_size):
            new_id = f'{prefix}{base_id + icnt}'
            new_doc = process_message(summary_template, new_id, icnt, stream, topic, cprocess)
            if stream == "direct":
                if len(bulk_docs) == batch_size:
                    flush_direct(conn, topic, bulk_docs)
                    bulk_docs = []
                    if interval > 0:
                        time.sleep(interval)
                bulk_docs.append(new_doc)
            else:
                loader.add(new_doc)
            if icnt % (batch_size * 5) == 0:
                end_time = datetime.datetime.now()
                time_diff = (end_time - istart_time)
                execution_time = time_diff.microseconds * 0.000001
                istart_time = datetime.datetime.now()
                bb.logit(f'[{cur_process.name}] {icnt} msgs, speed: {(batch_size * 5)/execution_time} msgs/sec')
            elif icnt % 20 == 0:
                #print(".", end="", flush=True)
                doo = "not"
            icnt += 1

    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.microseconds * 0.000001
    bb.logit(f'[{cur_process.name}] Complete {icnt} msgs, speed: {(batch_size * num_iterations)/execution_time} msgs/sec')
    
    # get the leftovers
    if stream == "direct":
        flush_direct(conn[settings["database"]],bulk_docs)
    bb.logit("#-------- COMPLETE -------------#")
    loader = None

def process_message(doc_template, new_id, cnt, stype, topic, cprocess):
    cur_doc = bb.read_json(doc_template)
    camp = f'C~{1000 + int(cnt/1000)}'
    global interval_vars
    age = random.randint(0,10)
    ctype = random.randint(0,9)
    dformat = "%Y-%m-%d %H:%M:%S"
    yr = 0
    month = 9
    month = month - age
    if month < 1:
        month = 12 + month
        yr = 1
    year = 2022 - yr
    day = random.randint(1,28)
    msgdt = datetime.datetime(year,month,day, 10, 45)
    if cnt == 0 or cnt % 100 == 0:
        interval_vars["constituent_identifier"] = f'{new_id}-{cprocess}'
    if cnt == 0 or cnt % 10000 == 0:
        interval_vars["campaign"] = fake.sentence()
        interval_vars["campaign_identifier"] = f'C~{1000000 + cnt}'
        interval_vars["vndr_nm"] = fake.bs()
    cur_doc["id"] = new_id
    cur_doc["cmnctn_identifier"] = f'{new_id}_{cur_doc["cmnctn_identifier"]}'
    cur_doc["constituent_identifier"] = interval_vars["constituent_identifier"]
    cur_doc["is_medicaid"] = "N"
    cur_doc["ext_taxonomy_identifier"] = f'{new_id}_{cur_doc["ext_taxonomy_identifier"]}'
    cur_doc["taxonomy_identifier"] = f'{new_id}_{cur_doc["taxonomy_identifier"]}'
    cur_doc["cmnctn_activity_dts"] = msgdt if stype == "direct" else msgdt.strftime(dformat)
    cur_doc["cmnctn_last_updated_dt"] = msgdt + datetime.timedelta(hours=4) if stype == "direct" else (msgdt + datetime.timedelta(hours=4)).strftime(dformat)
    cur_doc["load_day"] = day
    cur_doc["load_month"] = month
    cur_doc["load_year"] = year
    cur_doc["campaign_name"] = interval_vars["campaign"]
    cur_doc["campaign_identifier"] = interval_vars["campaign_identifier"]
    cur_doc["taxonomy_cmnctn_format"] = random.choice(["Email", "SMS", "Call", "Popup", "DirectMail","notification"])
    cur_doc["taxonomy_cmnctn_content_topic"] = fake.bs()
    cur_doc["taxonomy_portfolio"] = random.choice(["Behavior Change/Next Best Action","Marketing Inquiry","Survey","Post-call quality check"])
    cur_doc["vndr_nm"] = interval_vars["vndr_nm"]
    cur_doc["version"] = settings["version"]
    cur_doc["archive_date"] = datetime.datetime.now() if stype == "direct" else datetime.datetime.now().strftime(dformat)
    cur_doc["create_date"] = datetime.datetime.now() if stype == "direct" else datetime.datetime.now().strftime(dformat)
    cur_doc["type"] = "comm_summary"
    if topic == "comm-detail":
        build_detail(cur_doc, new_id)
    if ctype > 8:
        cur_doc["is_medicaid"] = "Y" 
    return(cur_doc)

def build_detail(doc, id):
    doc["cmnctn_detail_id"] = "41~2650905331^MEA^1757^1479^One Time Password Notification^Member Password Change and Notification^CT-0000001837^COMETS"
    doc["cmnctn_activity_status_desc"] = "Sent to Archive, Request Received, Request sent to SMS vendor, Request Rcvd For Archive, Sent To OMS, Filenet load Successful, Sent To OMS Archive"
    doc["cmnctn_activity_url"] = fake.url()
    doc["cmnctn_content_id"] = ""
    doc["cmnctn_transcripts"] = "N/A"
    doc["cmnctn_send_performance_desc"] = fake.bs()
    doc["src_cmnctn_id"] = ""
    doc["src_cmnctn_parent_purpose_name"] = ""
    doc["src_cmnctn_child_purpose_name"] = ""
    doc["src_cmnctn_parent_display_name"] = ""
    doc["src_cmnctn_child_display_name"] = ""
    doc["cmnctn_detail_name1"] = fake.word()
    doc["cmnctn_detail_value1"] = fake.word()
    doc["cmnctn_detail_name2"] = fake.word()
    doc["cmnctn_detail_value2"] = fake.word()
    doc["cmnctn_detail_name3"] = fake.word()
    doc["cmnctn_detail_value3"] = fake.word()
    doc["cmnctn_detail_name4"] = fake.word()
    doc["cmnctn_detail_value4"] = fake.word()
    doc["cmnctn_detail_name5"] = fake.word()
    doc["cmnctn_detail_value5"] = fake.word()
    doc["type"] = "comm_detail"
    return doc

def flush_direct(conn,topic, docs):
    db = conn[settings["database"]]
    coll = settings["topics"][topic] 
    ans = db[coll].insert_many(docs)
    bb.logit(f"Loading batch: {len(docs)}")
    docs = []

# --------------------------------------------------------- #
#  Cosmos Compare

def basic_compare():
    base_counter = 1000000
    num_records = 10000
    prefix = "MDB"
    global interval_vars
    interval_vars = {}
    database = settings["database"]
    collection = "summary"
    summary_template = settings["summary_template"]
    meth = "uri"
    if "cosmos" in ARGS:
        meth = "cosmos"
        prefix = "COSMOS"
        collection = collection + meth
    conn = client_connection(meth)
    db = conn[database]
    IDGEN.set({"seed" : base_counter, "size" : num_records, "prefix" : prefix})
    icnt = 0
    start_time = datetime.datetime.now()
    istart_time = datetime.datetime.now()
    base_id = int(IDGEN.get(prefix, num_records).replace(prefix,""))
    for k in range(num_records):
        new_id = f'{prefix}{base_id + icnt}'
        new_doc = process_message(summary_template, new_id, icnt, "direct", "summary", "p1")
        db[collection].insert_one(new_doc)
        if icnt % 1000 == 0:
            end_time = datetime.datetime.now()
            time_diff = (end_time - istart_time)
            execution_time = time_diff.microseconds * 0.000001
            istart_time = datetime.datetime.now()
            bb.logit(f'#--- Partial 1000 msgs, speed: {(1000)/execution_time} msgs/sec')
        if icnt % 10 == 0:
            bb.logit(f'Recs - {k} of {num_records}')
        icnt += 1
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.microseconds * 0.000001
    bb.logit(f'#--- Complete {icnt} msgs, speed: {(icnt)/execution_time} msgs/sec')
 


# --------------------------------------------------------- #
#  Pub Sub Subscription
# --------------------------------------------------------- #

def message_subscription():
    # read settings and echo back
    bb.message_box("Pub/Sub Message Subscriiption", "title")
    bb.logit(f'# Settings from: {settings_file}')
    # Spawn processes
    num_procs = settings["process_count"]
    jobs = []
    inc = 0
    # multiprocessing.set_start_method("fork", force=True)
    # for item in range(num_procs):
    #     p = multiprocessing.Process(target=worker_message_subscribe, args = (item,))
    #     jobs.append(p)
    #     p.start()
    #     time.sleep(1)
    #     inc += 1
    worker_message_subscribe()

    # main_process = multiprocessing.current_process()
    # bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    # for i in jobs:
    #     i.join()

def worker_message_subscribe(num_iterations):
    '''
        read from pub/sub
        (batch size)
            push to mongoDB (bulk op) (collection per topic)

        single-instance now
        gke - scale up
    
    '''
    #  Send a copy of the claim with one or two field changes
    #  Add an updateDate and sequencenumber, updateName = TCD-1
    # cur_process = multiprocessing.current_process()
    global g_loader
    global icnt
    tester = False
    if "test" in ARGS:
        tester = True
    feed = 1
    if "feed" in ARGS:
        feed = int(ARGS["feed"])
        sample_size = 10
        num_iterations = int(ARGS["feed"])
        interval = 5
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    project = settings["gcp"]["pub_sub_project"]
    subscription = settings["gcp"]["pub_sub_subscription"]
    timeout = settings["gcp"]["pub_sub_timeout"]
    bb.logit(f'Subscriber set in {project} for topic: {subscription}')
    g_loader = DbLoader({"settings" : settings})
    icnt = 0
    
    start_time = datetime.datetime.now()
    feed = False
    keep_going = True
    # consumer function to consume messages from a topics for a given timeout period
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project, subscription)
    bb.logit(f"Listening for messages on {subscription_path}..\n")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=process_payload)
    base_counter = settings["base_counter"]
    batch_size = settings["batch_size"]
    while keep_going:
        #bb.logit(f'Iter: {k} of {num_iterations}, {sample_size} per batch')
        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.                
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                streaming_pull_future.cancel()
                keep_going = False

    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    #file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")   
    bb.logit("#-------- COMPLETE -------------#")

# callback function for procesrsing consumed payloads 
# prints recieved payload
def process_payload(message):
    global icnt
    new_doc = json.loads((message.data).decode())
    if "_id" in new_doc:
        new_doc["old_id"] = new_doc["_id"]
        new_doc.pop("_id", None)    
    new_doc["_id"] = new_doc["taxonomy_identifier"]
    bb.logit(f'Message[{icnt}] {new_doc["taxonomy_identifier"]}.')
    g_loader.add(new_doc)
    icnt += 1
    message.ack()    

#----------------------------------------------------------------------#
#   Reporting
#----------------------------------------------------------------------#
def perf_stats():
    conn = client_connection()
    db = conn[settings["database"]]
    cc = PerfQueries({"args" : ARGS, "settings" : settings, "db" : db})
    cc.perf_stats()

def indexing_stats():
    num_to_do = 10
    prefix = "TEST1"
    base = "PerfTest"
    topic = "comm-summary"
    summary_template = settings["summary_template"]
    base_counter = settings["base_counter"]
    global interval_vars
    interval_vars = {}
    conn = client_connection()
    db = conn[settings["database"]]
    coll = "comm_summary" 
    loader = MessageLoader(topic, {"settings": settings})
    IDGEN.set({"seed" : base_counter, "size" : num_to_do, "prefix" : prefix})
    base_id = int(IDGEN.get(prefix, num_to_do).replace(prefix,""))
    istart_time = datetime.datetime.now()
    start_time = datetime.datetime.now()
    for inc in range(num_to_do):

        new_doc = process_message(summary_template, base_id + inc, inc, "kafka", topic, "p-1")
        new_doc["vndr_nm"] = f'{base}-{inc}'
        loader.add_kafka(new_doc)
        if (inc + 1) % 10 == 0:
            end_time = datetime.datetime.now()
            time_diff = (end_time - istart_time)
            execution_time = time_diff.microseconds * 0.000001
            istart_time = datetime.datetime.now()
            bb.logit(f'Processed: {inc + 1}, ToDo: {num_to_do}, elapsed: {execution_time}')
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.microseconds * 0.000001
    bb.logit(f'LoadComplete: {inc + 1}, ToDo: {num_to_do}, elapsed: {execution_time}')
    # Now check for records in index
    pipe = [
        {"$search" : {
            "compound" : {
                "must" : [
                    {"regex" : {"query" : f"{base}.*", "path" : "vndr_nm", "allowAnalyzedField": True}}
                ]
            }
        }},
        {"$project" : {
            "vendor_name" : 1, "last_modified_at": 1, "constituent_identifier" : 1
        }},
        {"$sort" : {
            "_id" : 1
        }},
        {"$count" : "numrecords"}
    ]
    start_time = datetime.datetime.now()
    for k in range(1000):
        #new_pipe = pipe.append({"$count" : "numrecords"})
        result = db[coll].aggregate(pipe)
        found = 0
        for k in result:
            if "numrecords" in k:
                found = k["numrecords"]
        bb.logit(f'Found: {k["numrecords"]}')
        if found == num_to_do:
            end_time = datetime.datetime.now()
            time_diff = (end_time - start_time)
            execution_time = time_diff.microseconds * 0.000001
            bb.logit(f'IndexingComplete: {num_to_do} found, elapsed: {execution_time}')
            pipe.pop(3)
            result = db[coll].aggregate(pipe)
            cnt = 0
            for rec in result:
                bb.logit(f'[{cnt}] vend: {rec["vendor_name"]}, ts: {rec["last_modified_at"].strftime("%H:%M:%s")}')
                cnt += 1
            break
        time.sleep(0.01)

def indexing_latency():
    num_to_do = 1000
    iters = 5
    prefix = "TEST1"
    base = "LatencyTest"
    topic = "comm-summary"
    summary_template = settings["summary_template"]
    base_counter = settings["base_counter"]
    global interval_vars
    interval_vars = {}
    conn = client_connection()
    db = conn[settings["database"]]
    coll = "comm_summary" 
    loader = MessageLoader(topic, {"settings": settings})
    IDGEN.set({"seed" : base_counter, "size" : num_to_do, "prefix" : prefix})
    base_id = int(IDGEN.get(prefix, num_to_do).replace(prefix,""))
    bulk_docs = []
    counter = 0
    istart_time = datetime.datetime.now()
    start_time = datetime.datetime.now()
    for iter in range(iters):
        bb.message_box(f'[iter: {iter}] - Processing {num_to_do} messages')
        for inc in range(num_to_do):
            new_doc = process_message(summary_template, base_id + inc, inc, "direct", topic, "p-1")
            new_doc["cmnctn_identifier"] = f'{base}{iter}-{counter}'
            bulk_docs.append(new_doc)
            counter += 1        
        flush_direct(conn, topic, bulk_docs)
        bulk_docs = []
        end_time = datetime.datetime.now()
        time_diff = (end_time - istart_time)
        execution_time = time_diff.microseconds * 0.000001
        istart_time = datetime.datetime.now()
        bb.logit(f'{num_to_do} msgs, speed: {(num_to_do)/execution_time} msgs/sec')
        wait_latency(f'{base}{iter}', num_to_do, db[coll]) 
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.microseconds * 0.000001
    bb.logit(f'LoadComplete: {inc + 1}, ToDo: {num_to_do * iters}, elapsed: {execution_time}')
 
 
def wait_latency(base, isize, coll):
    # Now check for records in index
    pipe = [
        {"$search" : {
            "index" : "limited",
            "compound" : {
                "must" : [
                    {"regex" : {"query" : f"{base}.*", "path" : "cmnctn_identifier", "allowAnalyzedField": True}}
                ]
            }
        }},
        {"$project" : {
            "cmnctn_identifier" : 1, "create_date": 1, "constituent_identifier" : 1
        }},
        {"$sort" : {
            "_id" : 1
        }},
        {"$count" : "numrecords"}
    ]
    start_time = datetime.datetime.now()
    for k in range(1000):
        #new_pipe = pipe.append({"$count" : "numrecords"})
        result = coll.aggregate(pipe)
        found = 0
        for k in result:
            if "numrecords" in k:
                found = k["numrecords"]
        bb.logit(f'Found: {found}')
        if found == isize:
            end_time = datetime.datetime.now()
            time_diff = (end_time - start_time)
            execution_time = time_diff.microseconds * 0.000001
            bb.logit(f'IndexingComplete: {isize} found, elapsed: {execution_time}')
            pipe.pop(3)
            result = coll.aggregate(pipe)
            cnt = 0
            for rec in result:
                if cnt > 50:
                    break
                bb.logit(f'[{cnt}] key: {rec["cmnctn_identifier"]}, ts: {rec["create_date"].strftime("%H:%M:%s")}')
                cnt += 1
            break
        time.sleep(0.01)
        
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
    if not "@" in mdb_conn:
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
        worker_load()
    elif ARGS["action"] == "load_updates":
        worker_activity_update_new(1)
    elif ARGS["action"] == "subscribe":
        message_subscription()
    elif ARGS["action"] == "publish_kafka":
        message_publisher("kafka")
    elif ARGS["action"] == "publish_direct":
        message_publisher("direct")
    elif ARGS["action"] == "publish_pubsub":
        message_publisher("pubsub")
    elif ARGS["action"] == "perf":
        # python3 commtracker_kafka.py action=perf batch=simple_match iters=20000
        perf_stats()
    elif ARGS["action"] == "index_stats":    
        indexing_stats()
    elif ARGS["action"] == "index_latency":    
        indexing_latency()
    elif ARGS["action"] == "compare":    
        basic_compare()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()

'''

'''
