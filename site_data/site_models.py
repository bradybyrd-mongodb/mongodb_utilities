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
    #  Reads EMR sample file and finds values
    cur_process = multiprocessing.current_process()
    pid = cur_process.pid
    conn = client_connection()
    bb.message_box(f"[{pid}] Worker Data", "title")
    IDGEN = args["idgen"]
    #settings = bb.read_json(settings_file)
    batch_size = settings["batch_size"]
    batches = settings["batches"]
    bb.logit('Current process is %s %s' % (cur_process.name, pid))
    start_time = datetime.datetime.now()
    db = conn[settings["database"]]
    #Signal = mix.Signal({"idgen" : IDGEN})
    #Asset = mix.AssetDetails({"idgen" : IDGEN})
    #Locale = mix.Locale({"idgen" : IDGEN})
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
        _details_["idgen"] = IDGEN
        prefix = _details_["job"]["id_prefix"]
        if "size" in _details_["job"] and _details_["job"]["size"] < batch_size:
            batch_size = _details_["job"]["size"]
        multiplier = _details_["job"]["multiplier"]
        count = int(batches * batch_size * multiplier)
        template_file = _details_["job"]["path"]
        #design = csvmod.doc_from_template(template_file, domain)
        base_counter = settings["base_counter"] + count * ipos
        IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefix})
        bb.logit(f'[{pid}] - {domain} | IDGEN - ValueHist: {IDGEN.value_history}')
        _details_["mixin"] = {}
        for obj in _details_["job"]["mix_objects"]:
            try:
                _details_["mixin"][obj["name"]] =  eval(obj["val"])
            except Exception as e:
                print(f'ERROR: evalobj: {obj}')
                print(str(e))
        bb.message_box(f'[{pid}] {domain} - base: {base_counter}', "title")
        tot = 0
        batches = int(count/batch_size)
        if batches == 0:
            batches = 1
        for cur_batch in range(batches):
            #bb.logit(f"[{pid}] - {domain} Loading batch: {cur_batch}")
            cnt = 0
            bulk_docs = build_batch_from_template(domain, {"batch" : cur_batch, "base_count" : base_counter, "size" : count})
            cnt += 1
            #print(bulk_docs)
            db[domain].insert_many(bulk_docs)
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

def build_batch_from_template(cur_coll, details = {}):
    template_file = _details_["job"]["path"]
    batch_size = settings["batch_size"]
    cur_info = {}
    if "size" in details and details["size"] < batch_size:
        batch_size = details["size"]
    if "cur_info" in details:
        cur_info = details["cur_info"]
    sub_size = 5
    cnt = 0
    records = []
    islist = False
    merger = Merger([
        (dict, "merge"),
        (list, zipmerge)
    ], [ "override" ], [ "override" ])
    for J in range(batch_size): # iterate through the bulk insert count
        # A dictionary that will provide consistent, random list lengths
        counts = random.randint(1, sub_size) #defaultdict(lambda: random.randint(1, 5))
        data = {}
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
                    counts = random.randint(1, sub_size) #defaultdict(lambda: random.randint(1, 5))
                #print(f"# -- Procpath {path}")
                partial = docpath(path, counts, row[2], cur_info) # Note, later version of files may not include required field
                #print(f'{row[0]}-{islist}: {partial}')
                # Merge partial trees.
                try:
                    data = merger.merge(data, partial)
                except Exception as e:
                    print("---- ERROR --------")
                    pprint.pprint(data)
                    print("---- partial --------")
                    pprint.pprint(partial)
                    print("---- error --------")
                    print(e)
                    exit(1)
                icnt += 1
                
        data = list(data.values())[0]
        data["doc_version"] = settings["version"]
        cnt += 1
        records.append(data)
    #bb.logit(f'{batch_size} {cur_coll} batch complete')
    return(records)

def master_from_file(file_name):
    return file_name.split("/")[-1].split(".")[0]

#----------------------------------------------------------------------#
#   CSV Loader Routines
#----------------------------------------------------------------------#

def docpath(path, counts, generator, cur_info):
    """Recursively walk a path, generating a partial tree with just this path's random contents"""
    mixin = {}
    if "mixin" in _details_:
        mixin = _details_["mixin"]
    stripped = stripProp(path[0])
    if len(path) == 1:
        # Base case. Generate a random value by running the Python expression in the text file
        #bb.logit(generator)
        if "_SAVE_" in generator:
            newgen = generator[:generator.find(")") + 1]
            leftover = generator.replace(newgen,"")
            cache = eval(newgen)
            result = { stripped: eval(str(cache) + leftover) }
        #elif "_CACHE_" in generator:
        #    cache = cur_info["cache"]
        #    result = eval(generator.replace("_CACHE_", str(newval)))
        else:
            #cur_info["cache"] = newgen
            result = { stripped: eval(generator) }
        return result
    elif path[0].endswith(')'):
        # Lists are slightly more complex. We generate a list of the length specified in the
        # counts map. Note that what we pass recursively is _the exact same path_, but we strip
        # off the ()s, which will cause us to hit the `else` block below on recursion.
        res = re.findall(r'\(.*\)',path[0])[0]
        if res == "()":
            lcnt = counts
        else:
            lcnt = int(res.replace("(","").replace(")",""))
        #print(f"lcnt: {lcnt}")
        return {            
            stripped: [ docpath([ path[0].replace(res,"") ] + path[1:], counts, generator, cur_info)[stripped] for X in range(0, lcnt) ]
        }
    else:
        # Return a nested page, of the specified type, populated recursively.
        return {stripped: docpath(path[1:], counts, generator, cur_info)}

#stripProp = lambda str: re.sub(r'\s+', '', (str[0].lower() + str[1:].strip('()')))
def stripProp(str):
    ans = str
    if str[0].isupper() and str[1].islower():
        ans = str[0].lower() + str[1:]
    if str.endswith(")"):
        stg = re.findall(r'\(.*\)',ans)[0]
        ans = ans.replace(stg,"")
    ans = re.sub(r'\s+', '', ans)
    return ans

def ser(o):
    """Customize serialization of types that are not JSON native"""
    if isinstance(o, datetime.datetime.date):
        return str(o)

def ID(key):
    id_map[key] += 1
    return key + str(id_map[key]+base_counter)

def zipmerge(the_merger, path, base, nxt):
    """Strategy for deepmerge that will zip merge two lists. Assumes lists of equal length."""
    return [ the_merger.merge(base[i], nxt[i]) for i in range(0, len(base)) ]

def local_geo():
    coords = fake.local_latlng('US', True)
    return [float(coords[1]), float(coords[0])]

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

def id_nfix():
    conn = client_connection()
    db = conn[settings["database"]]
    bb.message_box(f"Reset IDs", "title")
    recs = db.location.find({},{"location_id" : 1, "_id" : 0})
    lowid = 1000001
    highid = 1000059
    for item in recs:
        print(f'Loc: {item["location_id"]}')
        db.location.update_one({"location_id" : item["location_id"]},{"$set": {"customer_id" : f'C-{random.randint(lowid,highid)}'}})

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

def id_fix3():
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

def id_fix4():
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
        # add location to asset
        #unset a random selection of assets
        # loca
        #sub_doc = []
        #if icnt % 100 == 0:
        #    sub_doc.append({"rtype" : "edge_device", "value": f'A-{random.randint(lowid,highid)}'})
        
        bulk_updates.append(
            UpdateMany({"location_id" : item["location_id"]},{"$set": {"location" : item["address"]["location"]}})
        )
        icnt += 1
    bulk_writer(db[coll], bulk_updates, "Stuff")

def id_fix():
    conn = client_connection()
    db = conn[settings["database"]]
    bb.message_box(f"Reset IDs", "title")
    coll = "asset"
    recs = db["asset"].aggregate([{"$sample": {"size": 20}},{"$project": {"asset_id": 1, "_id": 0}}])
    ids = []
    for k in recs:
        ids.append(k["asset_id"])
    highid = 1001999
    bulk_updates = []
    icnt = 0
    print(ids)
    results = db[coll].update_many({"asset_id": {"$in": ids}},{"$unset" : {"location_id": ""}})
    print(f'Found: {results.matched_count}, Updated: {results.modified_count}')

def move_assets():
    conn = client_connection()
    db = conn[settings["database"]]
    location_id = "L-1000087"
    newlocation_id = "L-1000087"
    bb.message_box(f"Moving assets to {newlocation_id}", "title")
    rec = db["location"].find_one({"location_id" : newlocation_id},{"location_id" : 1, "address.location" : 1, "_id" : 0})
    coll = "asset"
    db[coll].udpate_many({"location_id" : location_id},{"$set": {"location_id" : newlocation_id, "location" : rec["address"]["location"]}})


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
        id_fix()
    elif ARGS["action"] == "query":
        run_query()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()