#!/usr/bin/python3
import sys
import csv
from collections import OrderedDict
from collections import defaultdict
import json
import logging
import datetime
import random
import uuid
import os
import io
import subprocess
import time
import re
import multiprocessing
import pprint
import faker
from pymongo import MongoClient
from contextlib import redirect_stdout
from bson.objectid import ObjectId
base_dir = os.path.dirname(os.path.abspath(__file__))
# apppend parent folder to path
sys.path.append(os.path.dirname(base_dir))
from bbutil import Util
settings_file = "perf_test_settings.json"

first_time = True

def coll_stats(counter, cur_colls, db):
    #  get colls stats on list of collections
    iters = settings["batch_size"]
    sample_time = settings["sample_time"]
    filter = "(system|vw_|_vw)"
    batch = []
    for inc in range(iters):
        for coll in cur_colls:
            if re.sub(filter,"",coll) == coll:
                res = db.command("collstats",coll)
                #bb.logit("--- Status Output ---")
                #bb.logit(res)
                print(".", end="", flush=True)
                res["timestamp"] = datetime.datetime.now()
                res["counter"] = counter
                batch.append(res)
                last_doc = res
                counter += 1
        time.sleep(sample_time)
    return batch

def collection_stats(p_num = 0):
    cur_process = multiprocessing.current_process()
    logging = True
    counter = 0
    if logging:
        uri = settings["logger"]["uri"]
        username = settings["logger"]["username"]
        password = settings["logger"]["password"]
        database = settings["logger"]["database"]
        batches = settings["batches"]
        batch_size = settings["batch_size"]
        testname = settings["testname"] + "_coll"
        if "testname" in ARGS:
            testname = ARGS["testname"]
        #shards = settings["source"]["shards"]
        uri = uri.replace("//", f'//{username}:{password}@')
        client = MongoClient(uri) #&w=majority
        mdb = client[database]
    interval = settings["sample_time"]
    user = settings["source"]["username"]
    pwd = settings["source"]["password"]
    s_uri = settings["source"]["uri"]
    s_uri = s_uri.replace("//", f'//{user}:{pwd}@')
    sdatabase = settings["source"]["database"]
    bb.message_box(f"[{cur_process.name}] Collection Stats", "title")
    sdbclient = MongoClient(s_uri)
    sdb = sdbclient[sdatabase]
    collections = sdb.list_collection_names()
    bb.logit(f'Source: {s_uri.replace(pwd, "**********")}')
    bb.logit(f'Logger: {uri.replace(pwd, "**********")}')
    bb.logit(f'Performing {batches} batches of {batch_size} items')
    bb.logit(f'Collections: {collections}')
    for iter in range(batches):
        bb.logit(f'Gathering stats {settings["batch_size"]} times using {interval} second interval')
        result = coll_stats(counter, collections, sdb)
        bb.logit(f'Batch {len(result)} items to do (collstats)')
        mdb[testname].insert_many(result)
        bb.logit(f'Add {batch_size} stats (tot: {counter})')
        counter += len(result)

    client.close()

def db_stats(counter, conn):
    iters = settings["batch_size"]
    batch = []
    for inc in range(iters):
        res = conn.admin.command("serverStatus")
        #bb.logit("--- Status Output ---")
        #bb.logit(res)
        print(".", end="", flush=True)
        del res["transportSecurity"]
        del res["metrics"]["aggStageCounters"]
        del res["$clusterTime"]
        res["timestamp"] = datetime.datetime.now()
        res["counter"] = counter
        if inc > 0: #"ok" in last_doc:
            res["opcounters"]["insertDelta"] = res["opcounters"]["insert"] - last_doc["opcounters"]["insert"]
            res["opcounters"]["updateDelta"] = res["opcounters"]["update"] - last_doc["opcounters"]["update"]
            res["opcounters"]["deleteDelta"] = res["opcounters"]["delete"] - last_doc["opcounters"]["delete"]
            res["opcounters"]["queryDelta"] = res["opcounters"]["query"] - last_doc["opcounters"]["query"]
            res["opcounters"]["getmoreDelta"] = res["opcounters"]["getmore"] - last_doc["opcounters"]["getmore"]
            res["opcounters"]["commandDelta"] = res["opcounters"]["command"] - last_doc["opcounters"]["command"]
            res["transactions"]["totalCommittedDelta"] = res["transactions"]["totalCommitted"] - last_doc["transactions"]["totalCommitted"]
            res["transactions"]["totalStartedDelta"] = res["transactions"]["totalStarted"] - last_doc["transactions"]["totalStarted"]
            res["transactions"]["totalAbortedDelta"] = res["transactions"]["totalAborted"] - last_doc["transactions"]["totalAborted"]
        else:
            res["opcounters"]["insertDelta"] = 0
            res["opcounters"]["updateDelta"] = 0
            res["opcounters"]["deleteDelta"] = 0
            res["opcounters"]["queryDelta"] = 0
            res["opcounters"]["getmoreDelta"] = 0
            res["opcounters"]["commandDelta"] = 0
            res["transactions"]["totalCommittedDelta"] = 0
            res["transactions"]["totalStartedDelta"] = 0
            res["transactions"]["totalAbortedDelta"] = 0
            first_time = False
        batch.append(res)
        last_doc = res
        counter += 1
        time.sleep(5)
    print(" done")
    return batch

def fix_vals(curdoc):
    curdoc["conn"] = int(curdoc["conn"])
    curdoc["flushes"] = int(curdoc["flushes"])
    curdoc["getmore"] = int(curdoc["getmore"])
    #curdoc["extra_infoSystem_time_us"] = int(curdoc.pop("extra_info.system_time_us"))
    #del curdoc["extra_info.system_time_us"]
    #curdoc["extra_infoUser_time_us"] = int(curdoc["extra_info.user_time_us"])
    #del curdoc["extra_info.user_time_us"]
    cur_trans = int(curdoc["transactions.totalCommitted"])
    if "host" in curdoc:
        bb.logit(f'Host: {curdoc["host"]}')
        host = curdoc["host"]
        diff = 0
        if host in last_vals:
            diff = cur_trans - last_vals[host]
        curdoc["transactionsNet"] = diff

    curdoc["transactionsTotalCommitted"] = cur_trans
    del curdoc["transactions.totalCommitted"]
    #curdoc["insert"] = int(curdoc["insert"].replace("*",""))
    #curdoc["update"] = int(curdoc["update"].replace("*",""))
    #curdoc["delete"] = int(curdoc["delete"].replace("*",""))
    curdoc["query"] = int(curdoc["query"].replace("*",""))
    curdoc["version"] = "1.0"
    return(curdoc)

def clean_key(key):
    return key.replace(":","-").replace(".","_").replace("@","").replace(",","")

def benchmark_queries():
    bb.message_box("Benchmark Queries", "title")
    uri = settings["source"]["uri"]
    username = settings["source"]["username"]
    password = settings["source"]["password"]
    database = settings["source"]["database"]
    batches = settings["batches"]
    batch_size = settings["batch_size"]
    bb.logit(f'Opening {uri}')
    bb.logit(f'Performing {batches} batches')
    uri = uri.replace("//", f'//{username}:{password}@')
    client = MongoClient(uri) #&w=majority
    mdb = client[database]
    tot = 0
    for batch in range(batches):
        bb.logit("Performing batch {batch}")
        for cnt in range(batch_size):
            for item,agg in settings["queries"].items():
                start = datetime.datetime.now()
                coll = item.split("_")[0]
                ans = mdb[coll].aggregate(agg)
                elapsed = datetime.datetime.now() - start
                bb.logit(f'{item} - result: {ans.first["numFound"]}, elapsed: {elapsed.seconds}')

#------ Demo Failover with Atlas Command --------------#
def failover():
    retry = False
    DB_NAME = "failtest"
    if "retry" in ARGS and ARGS["retry"] == "true":
        retry = True
    uri = settings["source"]["uri"]
    bb.message_box("Continuous load test", "title")
    connection = client_connection("source.uri", {"retry" : retry})
    db = connection[DB_NAME]
    #db.records.create_index('date_created', name=TTL_INDEX_NAME, expireAfterSeconds=7200)
    #print('Ensured there is a TTL index to prune records after 2 hours\n')
    connect_problem = False
    count = 0
    start_time = datetime.datetime.now()
    last_time = datetime.datetime.now()
    # Just ctrl-c to cancel
    while True:
        try:
            if (count % 10 == 0):
                curt = datetime.datetime.now()
                bb.logit(f'INSERT {count}, elapsed: {(curt - last_time).total_seconds()}')
                last_time = curt
            db.records.insert_one({
                'val': random.randint(1, 100),
                'date_created': datetime.datetime.utcnow()
            })

            count += 1

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

def real_time_stats(p_num = 0):
    cur_process = multiprocessing.current_process()
    logging = True
    interval = settings["sample_time"]
    counter = 0
    if logging:
        uri = settings["logger"]["uri"]
        if "key" in settings["logger"]:
            bb.logit("using key")
            secret = bb.desecret(settings["logger"]["key"])
            username = secret.split(":")[0]
            password = secret.split(":")[1]
        else:
            username = settings["logger"]["username"]
            password = settings["logger"]["password"]
        database = settings["logger"]["database"]
        batches = settings["batches"]
        batch_size = settings["batch_size"]
        testname = settings["testname"]
        if "testname" in ARGS:
            testname = ARGS["testname"]
        #shards = settings["source"]["shards"]
        uri = uri.replace("//", f'//{username}:{password}@')
        client = MongoClient(uri) #&w=majority
        mdb = client[database]
    suser = settings["source"]["username"]
    spwd = settings["source"]["password"]
    s_uri = settings["source"]["uri"]
    s_uri = s_uri.replace("//", f'//{suser}:{spwd}@')
    sdb = MongoClient(s_uri)
    
    bb.message_box(f"[{cur_process.name}] Real-Time Stats", "title")
    bb.logit(f'Source {s_uri.replace(spwd, "**********")}')
    bb.logit(f'Logger {uri.replace(password, "**********")}')
    bb.logit(f'Performing {batches} batches of {batch_size} items')
    for iter in range(batches):
        bb.logit(f'Gathering stats {settings["batch_size"]} times using {interval} second interval')
        result = db_stats(counter, sdb)
        #bb.logit(f'Batch {len(result)} items to do')
        mdb[testname].insert_many(result)
        bb.logit(f'Add {batch_size} stats (tot: {counter})')
        counter += len(result)

    client.close()

#-----------------------------------------------------------#
#  DataDog Alerts Module
#-----------------------------------------------------------#

def monitoring():
    # read settings and echo back
    bb.message_box("Monitoring Loader", "title")
    bb.logit(f'# Settings from: {settings_file}')
    # Spawn processes
    num_procs = 2
    jobs = []
    inc = 0
    multiprocessing.set_start_method("fork", force=True)
    for item in range(num_procs):
        print(f'Item: {item}')
        if item == 0:
            p = multiprocessing.Process(target=real_time_stats, args = (item,))
        else:
            p = multiprocessing.Process(target=collection_stats, args = (item,))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def alert_notifier():
    '''
        Perform aggregation for this week vs last week numbers
        use criticality, abs size and growth as factors
        notify webhook on datadog
        BJB 10/5/22
    '''
    conn = client_connection()
    database = settings["database"]
    coll = settings["collection"]
    cur = datetime.datetime.now()
    prev = cur - datetime.timedelta(days=7)
    pipe = [
    {
        '$match': {
            '$or': [
                {
                    'timestamp': {
                        '$gt': (cur - datetime.timedelta(minutes=10)), 
                        '$lt': cur
                    }
                }, {
                    'timestamp': {
                        '$gt': (prev - datetime.timedelta(minutes=10)), 
                        '$lt': prev
                    }
                }
            ]
        }
    }, {
        '$sort': {
            'timestamp': 1
        }
    }, {
        '$group': {
            '_id': '$ns', 
            'criticality': {
                '$first': '$meta.criticality'
            }, 
            'busyness': {
                '$last': '$meta.busyness'
            }, 
            'prev_ts': {
                '$first': '$timestamp'
            }, 
            'curSize': {
                '$last': '$totalSize'
            }, 
            'prevSize': {
                '$first': '$totalSize'
            }, 
            'cur_ts': {
                '$last': '$timestamp'
            }
        }
    }
    ]
    recs = conn[database][coll].aggregate(pipe)
    alerts = settings["alerts"]
    for doc in recs:
        size = doc['curSize']
        growth = (size - doc['prevSize'])/doc['prevSize']
        if size > alerts["size"] and growth > alerts["growth"]:
            send_alert(f'collection: {doc["ns"]} - reaching critical size={size}, growth={growth}')

def send_alert(msg):
    # send message to data dog
    boo = "boo"


#-----------------------------------------------------------#
#  Utility Code
#-----------------------------------------------------------#

def client_connection(type = "uri", details = {}):
    if "." in type:
        parts = type.split(".")
        mdb_conn = settings[parts[0]][parts[1]]
        username = settings[parts[0]]["username"]
        password = settings[parts[0]]["password"]
    else:
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

#------------------------------------#
#  Rest
def rest_get(url, details = {}):
  headers = {"Content-Type" : "application/json", "Accept" : "application/json" }
  if "headers" in details:
      headers = details["headers"]
  api_pair = bb.desecret(api_key).split(":")
  response = requests.get(url, auth=HTTPDigestAuth(api_pair[0], api_pair[1]), headers=headers)
  result = response.content.decode('ascii')
  if "verbose" in details:
      bb.logit(f"Status: {response.status_code}")
      bb.logit(f"Headers: {response.headers}")
      bb.logit(f"URL: {url}")
      bb.logit(f"Response: {result}")
  return(json.loads(result))

def rest_get_file(url, details = {}):
  # https://stackoverflow.com/questions/36292437/requests-gzip-http-download-and-write-to-disk
  headers = {"Content-Type" : "application/json", "Accept" : "application/json" }
  if "headers" in details:
      headers = details["headers"]
  api_pair = bb.desecret(api_key).split(":")
  local_filename = details["filename"]
  try:
      response = requests.get(url, auth=HTTPDigestAuth(api_pair[0], api_pair[1]), headers=headers, stream=True)
  except Exception as e:
      print(e)
  raw = response.raw
  with open(local_filename, 'wb') as out_file:
    cnt = 1
    while True:
        chunk = raw.read(1024, decode_content=True)
        if not chunk:
            break
        bb.logit(f'chunk-{cnt}')
        out_file.write(chunk)
        cnt += 1
  '''
  with requests.get(url, auth=HTTPDigestAuth(api_pair[0], api_pair[1]), headers=headers, stream=True) as r:

    r.raise_for_status()
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            # If you have chunk encoded response uncomment if
            # and set chunk_size parameter to None.
            #if chunk:
            f.write(chunk)
  '''
  if "verbose" in details:
      bb.logit(f"URL: {url}")
  return(local_filename)

def rest_post(url, details = {}):
  headers = {"Content-Type" : "application/json", "Accept" : "application/json"}
  if "headers" in details:
      headers = details["headers"]
  api_pair = bb.desecret(api_key).split(":")
  post_data = details["data"]
  response = requests.post(url, auth=HTTPDigestAuth(api_pair[0], api_pair[1]), data=json.dumps(post_data), headers=headers)
  result = response.json() #content.decode('ascii')
  if "verbose" in details:
      bb.logit(f"Status: {response.status_code}")
      bb.logit(f"Headers: {response.headers}")
      bb.logit(f"Response: {json.dumps(result)}")
  return(result) #json.loads(result))

def rest_update(url, details = {}):
  headers = {"Content-Type" : "application/json", "Accept" : "application/json"}
  api_pair = bb.desecret(api_key).split(":")
  post_data = details["data"]
  if isinstance(post_data, str):
      post_data = json.loads(post_data)
      print(post_data)
  response = requests.patch(url, auth=HTTPDigestAuth(api_pair[0], api_pair[1]), data=json.dumps(post_data), headers=headers)
  result = response.json() #content.decode('ascii')
  if "verbose" in details:
      bb.logit(f"Status: {response.status_code}")
      bb.logit(f"Headers: {response.headers}")
      bb.logit(f"Response: {json.dumps(result)}")
  return(result) #json.loads(result))


#-----------------------------------------------------------#
#------------------------  MAIN ----------------------------#
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    base_counter = settings["base_counter"]
    if "base" in ARGS:
        base_counter = int(ARGS["base"])

    if "action" not in ARGS:
        monitoring()
    elif ARGS["action"] == "coll_stats":
        collection_stats()
    elif ARGS["action"] == "server_stats":
        real_time_stats()
    elif ARGS["action"] == "encrypt":
        if "secret" in ARGS:
            result = bb.secret(ARGS["secret"])
            bb.logit(result)
        else:
            bb.logit("Enter arg secret=user:password")
    elif ARGS["action"] == "perf":
        bb.logger("runnning performance test")
        benchmark_queries()
    elif ARGS["action"] == "fail":
        # > python3 perf_stats.py action=fail retry=true
        failover()
'''
Aggregation:
- create a merged collection
pipe = [
    
]


#---------------------------#
#  Script to keep it running - Cron
#!/bin/bash
#  Call with cron:
#  */15 * * * *      mongo_monitor.sh
base_dir="/home/brady_byrd/code"
if [ $(ps -efa | grep -v grep | grep real_time_stats -c) -gt 0 ] ;
then
    echo "Process running ...";
else
    cd $base_dir
    echo "Starting real_time_stats.py"
    python3 real_time_stats.py
fi;

#--------------------------------------#
#  Collection Aggregations
index sizes:
[
    {"$match" : {"ns" : {"$regex" : "^foodie.*"}}},
    {"$project: {

    }}
]

Index Sizes
[
    {
        '$match': {
            'ns': re.compile(r"^foodie.*")

        }
    }, {
        '$project': {
            'ns': 1, 
            'timestamp': 1, 
            'isizes': {
                '$objectToArray': '$indexSizes'
            }
        }
    }, {
        '$unwind': {
            'path': '$isizes'
        }
    }, {
        '$project': {
            'ns': 1, 
            'timestamp': 1, 
            'index_name': '$isizes.k', 
            'index_size': '$isizes.v'
        }
    }
]

# ------------------------------------------- #
#  Scatter Graph

[
    {
        '$match': {
            '$or': [
                {
                    'timestamp': {
                        '$gt': datetime(2022, 9, 22, 12, 0, 0, tzinfo=timezone.utc), 
                        '$lt': datetime(2022, 9, 23, 12, 0, 0, tzinfo=timezone.utc)
                    }
                }, {
                    'timestamp': {
                        '$gt': datetime(2022, 9, 13, 12, 0, 0, tzinfo=timezone.utc), 
                        '$lt': datetime(2022, 9, 14, 12, 0, 0, tzinfo=timezone.utc)
                    }
                }
            ]
        }
    }, {
        '$sort': {
            'timestamp': 1
        }
    }, {
        '$group': {
            '_id': '$ns', 
            'prevts': {
                '$first': '$timestamp'
            }, 
            'curSize': {
                '$last': '$totalSize'
            }, 
            'prevSize': {
                '$first': '$totalSize'
            }, 
            'curts': {
                '$last': '$timestamp'
            }
        }
    }
]

- paster
[
    {
        '$match': {
            '$or': [
                {
                    'timestamp': {
                        '$gt': ISODate("2022-09-22T12:00:00"), 
                        '$lt': ISODate("2022-09-23T12:00:00")
                    }
                }, {
                    'timestamp': {
                        '$gt': ISODate("2022-09-15T12:00:00"), 
                        '$lt': ISODate("2022-09-16T12:00:00")
                    }
                }
            ]
        }
    }, {
        '$sort': {
            'timestamp': 1
        }
    }, {
        '$group': {
            '_id': '$ns', 
            'criticality': {
                '$first': '$meta.criticality'
            }, 
            'busyness': {
                '$last': '$meta.busyness'
            }, 
            'prev_ts': {
                '$first': '$timestamp'
            }, 
            'curSize': {
                '$last': '$totalSize'
            }, 
            'prevSize': {
                '$first': '$totalSize'
            }, 
            'cur_ts': {
                '$last': '$timestamp'
            }
        }
    }
]

#------------------------------#
#  Fix meta
db.monitoring_coll.updateMany({ns: "commtracker.comm_summary"},{$set: {meta: {"criticality": 9, "busyness": 88}}})
db.monitoring_coll.updateMany({ns: "commtracker.comm_detail"},{$set: {meta: {"criticality": 7, "busyness": 52}}})
db.monitoring_coll.updateMany({ns: "commtracker.messages"},{$set: {meta: {"criticality": 4, "busyness": 22}}})
db.monitoring_coll.updateMany({ns: "commtracker.messages1"},{$set: {meta: {"criticality": 3, "busyness": 90}}})

#-------------------------------#
#  DataDog
mdb email / h4
API key: 967880f4c28b3bf790de19a2fd13b37c
Application Key
bbExport: cd89ef98a0e18facc8c9147f3f0b5df74d64948d
'''
