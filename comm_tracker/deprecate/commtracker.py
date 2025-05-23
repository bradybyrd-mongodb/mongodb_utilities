from message_loader import MessageLoader
from mongo_loader import DbLoader
from id_generator import Id_generator
from bbutil import Util
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
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
base_dir = os.path.dirname(os.path.abspath(__file__))
# apppend parent folder to path
sys.path.append(os.path.dirname(base_dir))
sys.path.append(os.path.join(base_dir, "templates"))
#from t_comm import CommFactory

fake = Faker()
letters = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L",
           "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"]
providers = ["cigna", "aetna", "anthem", "bscbsma", "kaiser"]

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
        p = multiprocessing.Process(target=worker_load, args=(item,))
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
    # worker_claim_load(pgcon,tables)
    # worker_rx_load(pgcon,tables)
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    #file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

# --------------------------------------------------------- #
#  Pub Sub Publish
# --------------------------------------------------------- #


def message_publisher(stream, topics):
    # read settings and echo back
    bb.message_box("Pub/Sub Message Subscriiption", "title")
    bb.logit(f'# Settings from: {settings_file}')
    # Spawn processes
    num_procs = settings["process_count"]
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


def worker_message_generate(proc_num, stream, topic):
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
    prefix = "COMT"
    feed = False
    loader = MessageLoader(topic, {"settings": settings})
    interval = 0
    base_counter = settings["base_counter"]
    batch_size = settings["batch_size"]
    num_records = settings["num_records"]
    summary_template = settings["summary_template"]
    num_iterations = int(num_records/batch_size)
    IDGEN.set({"seed": base_counter, "size": num_records, "prefix": prefix})
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
    elif stream == "pubsub":
        loader.add = loader.add_pubsub

    for k in range(num_iterations):
        #bb.logit(f'Iter: {k} of {num_iterations}, {batch_size} per batch - total: {icnt}')
        icnt = 0
        base_id = int(IDGEN.get(prefix, num_records).replace(prefix, ""))
        for row in range(batch_size):
            new_id = f'{prefix}{base_id + icnt}'
            new_doc = process_message(summary_template, new_id)
            loader.add(new_doc)
            icnt += 1
        # get the leftovers
        loader.flush()

    bb.logit("#-------- COMPLETE -------------#")
    loader = None


def process_message(doc_template, new_id):
    cur_doc = bb.read_json(doc_template)
    age = random.randint(0, 6)
    yr = 0
    month = 9
    month = month - age
    if month < 1:
        month = 12 + month
        yr = 1
    year = 2022 - yr
    day = random.randint(1, 28)
    msgdt = datetime.datetime(year, month, day, 10, 45)
    cur_doc["id"] = new_id
    cur_doc["cmnctn_identifier"] = f'{new_id}_{cur_doc["cmnctn_identifier"]}'
    cur_doc["ext_taxonomy_identifier"] = f'{new_id}_{cur_doc["ext_taxonomy_identifier"]}'
    cur_doc["taxonomy_identifier"] = f'{new_id}_{cur_doc["taxonomy_identifier"]}'
    cur_doc["cmnctn_activity_dts"] = msgdt.strftime("%Y-%m-%dT%H:%M:%S")
    cur_doc["cmnctn_last_updated_dt"] = (
        msgdt + datetime.timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%S")
    cur_doc["load_day"] = day
    cur_doc["load_month"] = month
    cur_doc["load_year"] = year
    cur_doc["campaign_name"] = fake.sentence()
    cur_doc["taxonomy_cmnctn_content_topic"] = fake.bs()
    cur_doc["taxonomy_portfolio"] = random.choice(
        ["Behavior Change/Next Best Action", "Marketing Inquiry", "Survey", "Post-call quality check"])
    return(cur_doc)

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
    g_loader = DbLoader({"settings": settings})
    icnt = 0

    start_time = datetime.datetime.now()
    feed = False
    keep_going = True
    # consumer function to consume messages from a topics for a given timeout period
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project, subscription)
    bb.logit(f"Listening for messages on {subscription_path}..\n")
    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=process_payload)
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
        print(
            f"No report called {report_type} choices: claim_history, claim_details")

#----------------------------------------------------------------------#
#   Utility Routines
#----------------------------------------------------------------------#


def client_connection(type="uri", details={}):
    mdb_conn = settings[type]
    username = settings["username"]
    password = settings["password"]
    if "username" in details:
        username = details["username"]
        password = details["password"]
    mdb_conn = mdb_conn.replace("//", f'//{username}:{password}@')
    bb.logit(f'Connecting: {mdb_conn}')
    if "readPreference" in details:
        client = MongoClient(
            mdb_conn, readPreference=details["readPreference"])  # &w=majority
    else:
        client = MongoClient(mdb_conn)
    return client


def increment_version(old_ver):
    parts = old_ver.split(".")
    return(f'{parts[0]}.{int(parts[1]) + 1}')


def check_file(type="delete"):
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
    IDGEN = Id_generator({"seed": base_counter})
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
    elif ARGS["action"] == "publish" and ARGS["stream"] == "kafka":
        if len(ARGS) == 3:
            bb.logit(f'Loading summary documents into topic: kafka-topic')
            message_publisher("kafka", ["kafka-topic"])
        elif len(ARGS) == 4:
            topics = ARGS["topics"].split(',')
            bb.logit(f'Loading summary documents into topics:' + topics[0] +"and" + topics[1])
            message_publisher("kafka", topics)


    elif ARGS["action"] == "publish" and ARGS["stream"] == "pubsub":
        message_publisher("pubsub")
    elif ARGS["action"] == "reports":
        claims_reports()
    else:
        print(f'{ARGS["action"]} not found')
    # conn.close()

'''
#---- Data Load ---------------------#
python3 claimcache.py action=customer_load
python3 claimcache.py action=recommendations_load
python3 claimcache_pbm.py action=load_claim_updates test=true size=10

{"user.user_id" : {$in: ["PROF1000083","PROF1000107","PROF1000123","PROF1000244","PROF1000255","PROF1000702"]}}

'''
