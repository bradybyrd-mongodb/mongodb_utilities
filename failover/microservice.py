#!/usr/bin/env python3

import os
import pymongo
import sys
import csv
from collections import OrderedDict
from collections import defaultdict
import json
import datetime
import time
import random
import demo_settings
import faker
import pprint
from bson.objectid import ObjectId
from bson.json_util import dumps
from bson.decimal128 import Decimal128
import bbhelper as bb
from pymongo import MongoClient
from faker import Faker

fake = Faker()
# App constants
DEBUG = True
settings = {}
collection = "none"
db = None
towns = ["Washington|DC|20014","McLean|VA|22037", "Arlington|VA|22141", "Tysons Corner|VA|22076", "Silver Spring|MD|20814", "Alexandria|VA|22141", "Bethesda|MD|20866", "Rockville|MD|20882","Mt Vernon|VA|22001", "Fairfax|VA|22003", "Kensington|MD|20811"]

# Runs a loop checking only v1.0 records
def microservice_one():
    bb.message_box("Microservice One - Executing", "title")
    ids = db[collection].distinct("employee_id")
    keep_going = True
    icnt = 0
    while keep_going:
        bb.logit("Running employees report (microservice one)")
        for i in range(5):
            item = db[collection].find({"employee_id" : random.choice(ids)}, {"_id": 0, "version": 1, "first_name": 1, "last_name": 1, "gender": 1})
            bb.logit(dumps(item))

        print("\n...")
        time.sleep(5)
    print("Operation completed successfully!!!")

#  Populates collection with v1.0 records
def create_documents():
    bb.message_box("Load Primary People", "title")
    collection = settings["collection"]
    db[collection].drop()
    db[collection].create_index([("employee_id", pymongo.ASCENDING) ], unique=True)
    batch_size = settings["batch_size"]
    count = settings["num_records"]
    batches = int(count/batch_size)
    tot = 0
    for batch in range(batches):
        docs = []
        for n in range(batch_size):
            docs.append(create_model(tot))
            tot += 1
        write_data(docs)
        bb.logit(f'completed {tot} records')

def create_model(item):
    # create a new employee document
    employee = OrderedDict()
    gender = random.choice(["F","M"])
    base_emp_no = 1000
    base_salary = 40000
    base_hire_year = 2000
    first_name = fake.first_name_male() if gender == "M" else fake.first_name_female()
    town = fake.random.choice(towns).split("|")

    employee = {
            "version" : "1.0",
            "employee_id": int(base_emp_no + item),
            "first_name": first_name,
            "last_name": fake.last_name(),
            "gender": gender,
            "title" : fake.job(),
            "annual_salary": base_salary * 0.67 + round(random.random() * base_salary),
            "total_savings" : Decimal128(str(random.randint(1000000, 1000000000) + 0.55)),
            "hire_date": datetime.datetime(int(base_hire_year + random.choice(range(17))),
                                            int(1 + (random.choice(range(11)))),
                                            int(1 + (random.choice(range(28))))),
            "addresses" : {
                "home" : {
                    "address" : fake.street_address(),
                    "city" : town[0],
                    "state" : town[1],
                    "zip" : town[2]
                    }
            }
    }

    return employee

def write_data(docs):
    #print(docs)
    result = db[collection].insert_many(docs)
    return result

def microservice_report():
    microservice("report")

def microservice(r_type = "load"):
    retry = False
    region = "EAST"
    database = settings["database"]
    collection = settings["collection"]
    if "retry" in ARGS and ARGS["retry"] == "true":
        retry = True
    if "region" in ARGS:
        region = ARGS["region"]
    bb.message_box(f"Microservice running - {region}", "title")
    connection = client_connection("mdb_uri", {"retry" : retry})
    db = connection[database]
    connect_problem = False
    count = 0
    elapsed = 0
    start_time = datetime.datetime.now()
    last_time = datetime.datetime.now()
    # Just ctrl-c to cancel
    while True:
        try:
            curt = datetime.datetime.now()                
            if (count % 10 == 0):
                elapsed = timer(last_time, count)
            else:
                elapsed = timer(last_time, count, "quiet")
            if r_type == "load":
                new_rec = create_model()
                new_rec["region"] = region
                new_rec["ts"] = curt
                new_rec["elapsed"] = elapsed
                db[collection].insert_one(new_rec)
            else:
                bb.logit("# ------------------- Employees report ----------- [{region}]")
                for i in range(5):
                    item = db[collection].find({"employee_id" : random.choice(ids)}, {"_id": 0, "version": 1, "first_name": 1, "last_name": 1, "gender": 1})
                    bb.logit(dumps(item))
            count += 1
            last_time = curt
            if (connect_problem):
                bb.logit(f'{datetime.datetime.now()} - RECONNECTED-TO-DB')
                connect_problem = False
            else:
                time.sleep(0.01)
        except KeyboardInterrupt:
            end_time = datetime.datetime.now()
            time_diff = (end_time - start_time)
            execution_time = time_diff.total_seconds()
            bb.logit(f"Testing took {execution_time} seconds")

            sys.exit(0)
        except Exception as e:
            bb.logit(f'{datetime.datetime.now()} - DB-CONNECTION-PROBLEM: '
                  f'{str(e)}')
            connect_problem = True
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    bb.logit(f"Testing took {execution_time} seconds")

# --------------------------------------------------------- #
#       UTILITY METHODS
# --------------------------------------------------------- #
def timer(starttime,cnt = 1, ttype = "sub"):
    elapsed = datetime.datetime.now() - starttime
    secs = elapsed.seconds
    msecs = elapsed.microseconds
    if secs == 0:
        elapsed = msecs * .001
        unit = "ms"
    else:
        elapsed = secs + (msecs * .000001)
        unit = "s"
    if ttype == "sub":
        logging.debug(f"query ({cnt} recs) took: {'{:.3f}'.format(elapsed)} {unit}")
    elif ttype = "quiet":
        quiet = "yes"
    else:
        logging.debug(f"# --- Complete: query took: {'{:.3f}'.format(elapsed)} {unit} ---- #")
        logging.debug(f"#   {cnt} items {'{:.3f}'.format((elapsed)/cnt)} {unit} avg")
    return elapsed

def client_connection(uri="mdb_uri", details = {}):
    try:
        mdb_conn = settings[uri]
        username = settings["username"]
        password = settings["password"]
        mdb_conn = mdb_conn.replace("//", f'//{username}:{password}@')
        if "retry" in details:
            client = MongoClient(mdb_conn, retryWrites=details["retry"]) #&w=majority
        else:
            client = MongoClient(mdb_conn)
        return client
    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to MongoDB: %s" % e)

    return client


#----------------------------------------------------#
#      MAIN
#----------------------------------------------------#

if __name__ == "__main__":
    ARGS = bb.process_args(sys.argv)
    settings_file = "microservice_settings.json"
    settings = bb.read_json(settings_file)
    base_counter = settings["base_counter"]
    collection = settings["default_collection"]
    conn = client_connection()
    db = conn[settings["database"]]

    if "base" in ARGS:
        base_counter = int(ARGS["base"])

    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "populate":
        create_documents()
    elif ARGS["action"] == "microservice":
        microservice_report()
    elif ARGS["action"] == "load_data":
        microservice()
    else:
        print(f'{ARGS["action"]} not found')
