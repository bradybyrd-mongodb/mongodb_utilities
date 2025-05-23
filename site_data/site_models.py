# ---------------------------------------------------------- #
#  Site Data
# ---------------------------------------------------------- #
'''
Tishman-Speyer
    Wang Plaza
        Building 1
            Chiller
                RPM
                input temp
                output temp
                flow
                rpm
                pump head pressure
            VAV
            AirHandler - main
            Calling-heat: 71
            Calling-cool: 36

        Building 2
        Building 3
        Conference Center

Portfolio Details
Sites:
Buildings:
    building_id:
    name:
    location:
        address
        geo
    size
    floors
    age
    portfolio:
        portfolio_id: 
        portfolio_name: Tishman,Simon,Cushman-Wakefield,Greystar,Jordans
        site_id: 
        site_name:
    assets:
        asset_id:
        type:
        name:
        make:
        model:
        age:
        location:
            desc:
            geo:
        coverage:
        condition:
        health_score:

Monitoring:
    building_id
    asset_id:
    asset_name:
    type:
    measurements:
        temp
        rotor_rpm
        input_temp
        output_temp
        output_pressure
        etc
        etc2
        

Strategy:
    generate portfolios and sites - keep in array
    generate buildings:
        x per portfolio/site
    generate measurements
        every x-mins

DevOps:
db.building.createIndex({
  "portfolio.portfolio_id": 1,
  "building_id": 1
})
db.building.createIndex({
  "location.coordinates": "2dsphere"
})
db.monitoring.createIndex({
  "building_id": 1,
  "asset_id": 1,
  "measurement_ts": 1
})
db.monitoring.updateMany({},
   [
    {
     "$set": {measurement_ts: {
        $dateAdd: {
            startDate: "$$NOW",
            unit: "day",
            amount: -1
            }
    }}}
  ])

  db.building.updateMany({},
   [
    {
     "$set": {"address.location.coordinates": "$address.location.coordinates.type"}
    }
  ])

# -------------------------------------------------- #
#   Aggregations
['B-2000026','B-2000203','B-2000486','B-2000884','B-2000542','B-2000926','B-2000426','B-2000491','B-2000375','B-2000757','B-2000254','B-2000989','B-2000459','B-2000309','B-2000226','B-2000545','B-2000093','B-2000706','B-2000631','B-2000414','B-2000015','B-2000674','B-2000256','B-2000554','B-2000760','B-2000094','B-2000967','B-2000774','B-2000580','B-2000307','B-2000911','B-2000415','B-2000319','B-2000906','B-2000585','B-2000403','B-2000145','B-2000054','B-2000950','B-2000698','B-2000740','B-2000456','B-2000328','B-2000523','B-2000869','B-2000711','B-2000896','B-2000257','B-2000151','B-2000434','B-2000215','B-2000725','B-2000828','B-2000918','B-2000372','B-2000506','B-2000250','B-2000079','B-2000941','B-2000190']

pipe = [
    {$sample: {size: 100}},
    {$project: { _id:0, building_id: 1}}
]
db.building.aggregate(pipe)

db.building.find({building_id: {$in: ['B-2000026','B-2000203','B-2000486','B-2000884','B-2000542','B-2000926']}})
db.monitoring.find({building_id: {$in: ['B-2000026','B-2000203','B-2000486','B-2000884','B-2000542','B-2000926']}})

db.building.find( {
   "address.location.coordinates": {
      $geoWithin: {
         $centerSphere: [
            [ -73.935242, 40.730610 ],
            3 / 6378.1
         ]
      }
   }
},{building_id: 1, "coords" : "$address.location.coordinates", "city": "$address.city",_id: 0 } )

-------
# Find all the buildings in a 3 mile radius of NYC with temp > 78degrees
[
  {
    $match: {
      "address.location.coordinates": {
        $geoWithin: {
          $centerSphere: [
            [-73.935242, 40.73061],
            3 / 6378.1
          ]
        }
      }
    }
  },
  {
    $project: {
      building_id: 1,
      coords: "$address.location.coordinates",
      _id: 0
    }
  },
  {
    $lookup: {
      from: "monitoring",
      let: {
        bldg_id: "$building_id"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ["$building_id", "$$bldg_id"]
            }
          }
        },
        {
          $project: {
            asset_id: 1,
            measurements: 1
          }
        },
        {
          $unwind: {
            path: "$measurements"
          }
        },
        {
          $project: {
            asset_id: 1,
            ts: "$measurements.timestamp",
            rotor_rpm: "$measurements.rotor_rpm",
            input_temp:
              "$measurements.input_temp",
            temp: "$measurements.temperature",
            alarm: "$measurements.alarm"
          }
        }
      ],
      as: "measurements"
    }
  },
  {
    $unwind:
      {
        path: "$measurements"
      }
  },
  {
    $match:
      {
        "measurements.temp": {
          $gt: 78
        }
      }
  }
]
'''
import sys
import os
import csv
#import vcf
from collections import OrderedDict
from collections import defaultdict
from deepmerge import Merger
#import itertools
import json
import random
import time
import re
import multiprocessing
import pprint
import copy
#import uuid
#import bson
from bson.objectid import ObjectId
from decimal import Decimal
import datetime
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
from pymongo import UpdateMany
from faker import Faker
base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(base_dir))
import process_csv_model as csvmod
from bbutil import Util
from id_generator import Id_generator
import bldg_mix as mix
fake = Faker()

settings_file = "site_settings.json"

def synth_data_load():
    # python3 site_models.py action=load_data
    multiprocessing.set_start_method("fork", force=True)
    bb.message_box("Loading Data", "title")
    bb.logit(f'# Settings from: {settings_file}')
    passed_args = {"ddl_action" : "info", "idgen": IDGEN}
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
    #  Called for each separate process
    cur_process = multiprocessing.current_process()
    random.seed()
    pid = cur_process.pid
    conn = client_connection()
    bb.message_box(f"[{pid}] Worker Data", "title")
    _details_["domain"] = "not-yet"
    _details_["job"] = {}
    _details_["id_generator"] = {}
    _details_["last_root"] = ""
    _details_["batching"] =  False
    _details_["mixin"] = {}
    _details_["batches"] = {}
        
    #IDGEN = args["idgen"]
    batch_size = settings["batch_size"]
    batches = settings["batches"]
    bb.logit('Current process is %s %s' % (cur_process.name, pid))
    start_time = datetime.datetime.now()
    db = conn[settings["database"]]
    bulk_docs = []
    tot = 0
    if "template" in args:
        template = args["template"]
        master_table = master_from_file(template)
        job_info = {master_table : {"path" : template, "multiplier" : 1, "id_prefix" : f'{master_table[0].upper()}-'}}
    else:
        job_info = settings["data"]
    # Loop through collection files
    #pprint.pprint(job_info)
    for domain in job_info:
        _details_["domain"] = domain
        _details_["job"] = job_info[domain]
        _details_["batches"] = {}
        batch_size = settings["batch_size"]
        prefix = _details_["job"]["id_prefix"]
        if "size" in _details_["job"] and _details_["job"]["size"] < batch_size:
            batch_size = _details_["job"]["size"]
        multiplier = _details_["job"]["multiplier"]
        count = int(batches * batch_size * multiplier)
        base_counter = settings["base_counter"] + count * ipos + 1
        id_generator("init", prefix, {"base": settings["base_counter"], "size": count, "cur_base": base_counter, "next" : base_counter})
        #IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefix})
        bb.logit(f'[{pid}] - {domain} | IDGEN - ValueHist: {_details_["id_generator"]}')
        for obj in _details_["job"]["mix_objects"]:
            try:
                _details_["mixin"][obj["name"]] =  eval(obj["val"])
            except Exception as e:
                print(f'ERROR: evalobj: {obj}')
                print(str(e))
        bb.message_box(f'[{pid}] {domain} - base: {base_counter}', "title")
        tot = 0
        batches = int(count/batch_size)
        if count < batch_size:
            batch_size = count
        batch_map = batch_digest_csv(domain)
        print("# ---------------------- Document Map ------------------------ #")
        pprint.pprint(batch_map) 
    
        if batches == 0:
            batches = 1
        for cur_batch in range(batches):
            bb.logit(f"[{pid}] - {domain} Loading batch: {cur_batch}, {batch_size} records")
            cnt = 0
            bulk_docs = build_batch_from_template(domain, batch_map, {"batch" : cur_batch, "base_count" : base_counter, "size" : batch_size})
            cnt += 1
            #print(bulk_docs)
            db[domain].insert_many(bulk_docs)
            #cur_ids = list(map(get_id, bulk_docs))
            #print(f'[{pid}] - CurIDs: {pprint.pformat(cur_ids)}')
            siz = len(bulk_docs)
            tot += siz
            bulk_docs = []
            cnt = 0
            #bb.logit(f"[{pid}] - {domain} Batch Complete: {cur_batch} - size: {siz}, Total:{tot}\nIDGEN - ValueHist: {IDGEN.value_history}")
            bb.logit(f"[{pid}] - {domain} Batch Complete: {cur_batch} - size: {siz}, Total:{tot}")
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    conn.close()
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def get_id(doc):
    return doc["_id"]

def build_batch_from_template(domain, batch_map, details = {}):
    batch_size = settings["batch_size"]
    if "size" in details and details["size"] < batch_size:
        batch_size = details["size"]
    cnt = 0
    records = []
    for J in range(batch_size): # iterate through the bulk insert count
        # A dictionary that will provide consistent, random list lengths
        data = batch_build_doc(domain, batch_map)
        data["doc_version"] = settings["version"]
        cnt += 1
        records.append(data)
    #bb.logit(f'{batch_size} {cur_coll} batch complete')
    return(records)

#----------------------------------------------------------------------#
#   CSV Loader Routines
#----------------------------------------------------------------------#
def batch_digest_csv(domain):
    template_file = _details_["job"]["path"]
    sub_size = 5
    islist = False
    last_root = ""
    batch_list = {}
    with open(template_file) as csvfile:
        propreader = csv.reader(csvfile)
        icnt = 0
        for row in propreader:
            if icnt == 0:
                icnt += 1
                continue
            #print(row)
            path = row[0].split('.')
            if ")" in row[0]: #path[-2].endswith('()'):  
                islist = True
                if "CONTROL" in row[0]:
                    counts = random.randint(1, int(row[1]))
                    icnt += 1
                    continue
            else:
                islist = False
            # Digest the csv into segments of field for assembly
            batch_accounting(batch_list, path, row[2], last_root)
            icnt += 1
        _details_["batches"] = batch_list
    return(batch_list)

def batch_accounting(batch_doc, path, generator, last_root):
    cur_root = ",".join(path[0:-1])
    action = "append"
    cdoc = {"field" : path[-1], "gen": generator}
    if last_root != cur_root:
        if cur_root not in batch_doc.keys():
            batch_doc[cur_root] = [cdoc]
            action = "new"
    if action == "append":
        batch_doc[cur_root].append(cdoc)
    batch_doc["last_root"] = cur_root

def batch_build_doc(collection, batch_map):
    # Start with base fields, go next level n-times
    doc = {}
    last_key = ""
    sub_size = 5
    counts = random.randint(1, sub_size)
    is_list = False
    #print(f'C: {collection}')
    for key, value in batch_map.items():
        parts = key.split(",")
        cur_base = key.replace(last_key, "")
        res = ""
        #print("# ------------------------------------------ #")
        #print(f'Key: {key}, cur: {cur_base}')
        if key.lower() == collection:
            for item in value:
                doc[item["field"]] = batch_generator_value(item["gen"])
        else:
            if parts[-1].endswith(')'):
                is_list = True
                res = re.findall(r'\(.*\)',cur_base)[0]
                if res == "()":
                    lcnt = counts
                else:
                    lcnt = int(res.replace("(","").replace(")",""))
            else:
                is_list = False
                lcnt = 1
            
            inc = len(parts[1:])
            #print(f"doing parts {inc}")
            #print(parts)
            if inc == 1:
                #print(f'Adding sub_arr to doc[{cleaned(parts[1])}]')
                doc[cleaned(parts[1])] = batch_sub(item, value, lcnt, is_list)
            elif inc == 2:
                #print(f'Adding sub_arr to doc[{cleaned(parts[1])}][{cleaned(parts[2])}]')
                if is_list:
                    for k in range(len(doc[cleaned(parts[1])])):
                        doc[cleaned(parts[1])][k][cleaned(parts[2])] = batch_sub(item, value, lcnt, is_list)
                else:
                    doc[cleaned(parts[1])][cleaned(parts[2])] = batch_sub(item, value, lcnt, is_list)
            elif inc == 3:
                # Ex: Asset.parents().location.type
                if part_is_list[parts[1]]:
                    for k in range(len(doc[cleaned(parts[1])])):
                        if part_is_list[parts[2]]:
                            for j in range(len(doc[cleaned(parts[2])])):
                                doc[cleaned(parts[1])][k][cleaned(parts[2])][j][cleaned(parts[3])] = batch_sub(item, value, lcnt, is_list)
                        else:
                            doc[cleaned(parts[1])][k][cleaned(parts[2])][cleaned(parts[3])] = batch_sub(item, value, lcnt, is_list)
                else:
                    if part_is_list[parts[2]]:
                        for j in range(len(doc[cleaned(parts[2])])):
                            doc[cleaned(parts[1])][cleaned(parts[2])][j][cleaned(parts[3])] = batch_sub(item, value, lcnt, is_list)
                        else:
                            doc[cleaned(parts[1])][cleaned(parts[2])][cleaned(parts[3])] = batch_sub(item, value, lcnt, is_list)
        last_key = key
    return(doc)

def batch_sub(item, fields, cnt, isarr):
    sub_arr = []
    for k in range(cnt):
        sub_doc = {}
        for item in fields:
            sub_doc[item["field"]] = batch_generator_value(item["gen"])
        sub_arr.append(sub_doc)
    if not isarr:
        sub_arr = sub_arr[0]
    return(sub_arr)

def cleaned(str):
    str = re.sub(r'\(.*\)','', str)
    return str.strip()

def part_is_list(part):
    return part.endswith(')')

def batch_generator_value(generator):
    mixin = {}
    if "mixin" in _details_:
        mixin = _details_["mixin"]
    try:
        if "_SAVE_" in generator:
            gen2 = generator.replace("_SAVE_","")
            newgen = gen2[:gen2.find(")") + 1]
            leftover = gen2.replace(newgen,"")
            cache = eval(newgen)
            _details_["cache"] = cache
            #bb.logit(f'Save: {str(cache) + leftover}')
            result = eval(str(cache) + leftover) 
        elif "_CACHE_" in generator:
            cache = _details_["cache"]
            #bb.logit(f'Cache: {generator.replace("_CACHE_", str(cache))}')
            result = eval(generator.replace("_CACHE_", str(cache)))
        else:
            #bb.logit(f'Eval: {generator}')
            result = eval(generator) 
    except Exception as e:
        print("---- ERROR --------")
        pprint.pprint(generator)
        print("---- error --------")
        print(e)
        exit(1)
    return(result)

def master_from_file(file_name):
    return file_name.split("/")[-1].split(".")[0]

#----------------------------------------------------------------------#
#   Data Helpers
#----------------------------------------------------------------------#
def local_geo():
    coords = fake.local_latlng('US', True)
    return [float(coords[1]), float(coords[0])]

def id_generator(action, prefix, details = {}):
    result = "none"
    if action != "init" and prefix not in _details_["id_generator"]:
        _details_["id_generator"][prefix] = {"base": 1000000, "size": 1000000, "cur_base": 1000000, "next" : 1000000}
    
    if action == "init":
        _details_["id_generator"][prefix] = {"base": details["base"], "size": details["size"], "cur_base": details["cur_base"], "next" : details["cur_base"]}
    elif action == "next":
         result =  f'{prefix}{_details_["id_generator"][prefix]["next"]}'
         _details_["id_generator"][prefix]["next"] += 1
    elif action == "batch":
        result =  _details_["id_generator"][prefix]["base"] + details["batch_size"]
        _details_["id_generator"][prefix]["next"] += details["batch_size"]
    elif action == "random":
        low = _details_["id_generator"][prefix]["cur_base"]
        high = _details_["id_generator"][prefix]["cur_base"] + _details_["id_generator"][prefix]["size"]
        result = f'{prefix}{fake.random_int(min=low,max=high)}'
    return result

def data_fixes():
    #cust_ids()
    unattached_assets()

def tester():
    _details_["id_generator"] = {}
    id_generator("init", "C-", {"base": 1000, "size": 100, "cur_base": 1000, "next" : 1000})
    bb.logit(f'IDGEN - ValueHist: {_details_["id_generator"]}')
    for inc in range(30):
        print(id_generator("random","C-"))
        
def cust_ids():
    # After loading, location_id, should align with customer_id in Asset and Location
    conn = client_connection()
    db = conn[settings["database"]]
    bb.message_box(f"Reset Location/Customer IDs", "title")
    recs = db.location.find({},{"location_id" : 1, "customer_id": 1, "address.location.coordinates": 1, "_id" : 0})
    lowid = 1000001
    highid = 1000059
    highlocid = 1000199
    bulk_updates = []
    loc_ids = []
    for item in recs:
        loc_ids.append([item["location_id"],item["customer_id"], item["address.location.coordinates"]])
        print(f'Loc: {item["location_id"]}')
        db.location.update_one({"location_id" : item["location_id"]},{"$set": {"customer_id" : f'C-{random.randint(lowid,highid)}'}})
    bb.message_box(f"Reset Asset/Location IDs", "title")
    icnt = 0
    while icnt < 2000:
        psize = random.randint(4,40)
        pipe = [{"$sample": {"size" : psize}},{"$project":{"asset_id": 1, "_id": 0}}]
        recs = db.asset.aggregate(pipe)
        ids = []
        for k in recs:
            ids.append(k["asset_id"])
        print(f'Sample - {psize}')
        pick = random.randint(lowid,highlocid)
        for item in recs:
            db.asset.update_many({"asset_id" : {"$in" : ids}},{"$set": {"location_id" : loc_ids[pick][0], "customer_id" : loc_ids[pick][1], "location.address.coordinates" : loc_ids[pick][2]}})
        icnt += psize
    conn.close()

def id_mfix():
    conn = client_connection()
    db = conn[settings["database"]]
    bb.message_box(f"Reset IDs", "title")
    coll = "asset"
    recs = db[coll].find({},{"asset_id" : 1, "_id" : 0})
    lowid = 1000001
    highid = 1000199
    bulk_updates = []
    for item in recs:
        #print(f'Loc: {item["asset_id"]}')
        bulk_updates.append(
            UpdateOne({"asset_id" : item["asset_id"]},{"$set": {"location_id" : f'L-{random.randint(lowid,highid)}'}})
        )
    bulk_writer(db[coll], bulk_updates, "Stuff")

def add_parents_to_asset():
    conn = client_connection()
    db = conn[settings["database"]]
    bb.message_box(f"Reset IDs", "title")
    coll = "asset"
    recs = db[coll].find({},{"asset_id" : 1, "_id" : 0})
    lowid = 1000001
    highid = 1001999
    bulk_updates = []
    icnt = 0
    for item in recs:
        #print(f'Loc: {item["asset_id"]}')

        sub_doc = [
            {"rtype" : "floor", "value": random.randint(1,25)},
            {"rtype" : "room", "value": f'r{random.randint(100,1500) + random.randint(10,80)}'}
        ]
        if icnt % 100 == 0:
            sub_doc.append({"rtype" : "edge_device", "value": f'A-{random.randint(lowid,highid)}'})
        
        bulk_updates.append(
            UpdateOne({"asset_id" : item["asset_id"]},{"$set": {"parents" : sub_doc}})
        )
        icnt += 1
    bulk_writer(db[coll], bulk_updates, "Stuff")

def add_geo_to_asset():
    conn = client_connection()
    db = conn[settings["database"]]
    bb.message_box(f"Reset IDs", "title")
    coll = "asset"
    recs = db["location"].find({},{"location_id" : 1, "address.location" : 1, "_id" : 0})
    lowid = 1000001
    highid = 1001999
    bulk_updates = []
    icnt = 0
    for item in recs:
        bulk_updates.append(
            UpdateMany({"location_id" : item["location_id"]},{"$set": {"location" : item["address"]["location"]}})
        )
        icnt += 1
    bulk_writer(db[coll], bulk_updates, "Stuff")

def unattached_assets():
    conn = client_connection()
    db = conn[settings["database"]]
    bb.message_box(f"Reset IDs", "title")
    coll = "asset"
    recs = db["asset"].aggregate([{"$sample": {"size": 100}},{"$project": {"_id": 1}}])
    ids = []
    for k in recs:
        ids.append(k["_id"])
    highid = 1001999
    bulk_updates = []
    icnt = 0
    print(ids)
    results = db[coll].update_many({"_id": {"$in": ids}},{"$unset" : {"location_id": ""}})
    print(f'Found: {results.matched_count}, Updated: {results.modified_count}')
    conn.close()

def move_assets():
    # Example to move assets to a new location
    conn = client_connection()
    db = conn[settings["database"]]
    location_id = "L-1000087"
    newlocation_id = "L-1000087"
    bb.message_box(f"Moving assets to {newlocation_id}", "title")
    rec = db["location"].find_one({"location_id" : newlocation_id},{"location_id" : 1, "address.location" : 1, "_id" : 0})
    coll = "asset"
    db[coll].udpate_many({"location_id" : location_id},{"$set": {"location_id" : newlocation_id, "location" : rec["address"]["location"]}})
    conn.close()

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
    db = conn[settings["database"]]
    num = len(quotes)
    cnt = 0
    for k in range(int(1000/num)):
        for curid in quotes:
            start = datetime.datetime.now()
            output = db.quote.find_one({"quote_id" : curid })
            if cnt % 100 == 0:
                bb.timer(start, 100)
                #bb.logit(f"{cur_process.name} - Query: Disease: {term} - Elapsed: {format(secs,'f')} recs: {output} - cnt: {cnt}")
            cnt += 1
            #time.sleep(.5)
    conn.close()

def client_connection(type = "uri", details = {}):
    mdb_conn = settings[type]
    username = settings["username"]
    password = settings["password"]
    if "secret" in password:
        password = os.environ.get("_PWD_")
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
    _details_ = {}
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
    elif ARGS["action"] == "load_file":
        load_template()
    elif ARGS["action"] == "load_data":
        synth_data_load()
    elif ARGS["action"] == "fix_data":
        data_fixes()
    elif ARGS["action"] == "tester":
        tester()
    elif ARGS["action"] == "query":
        run_query()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()