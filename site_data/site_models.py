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

from bbutil import Util
from id_generator import Id_generator

fake = Faker()

settings_file = "site_settings.json"

class CurItem:
    # Use an instance to track the current record being processed
    def __init__(self, details = {}):
        self.addr_info = {}
        self.sites = []
        self.id_map = {}
        self.cur_id = False
        self.ipos = 0
        self.version = "1.0"
        self.counter = 0
        if "addr_info" in details:
            self.addr_info = details["addr_info"]
        if "sites" in details:
            self.sites = details["sites"]
        if "version" in details:
            self.version = details["version"]

    def set_addr_info(info):
        self.addr_info = info

    def set_cur_id(id_val):
        #for each record generated, store the current id as a local value so you can lookup multiple 
        #items
        self.cur_id = id_val
        if self.counter >= len(sites):
            self.counter = 0 
        self.counter += 1
        self.ipos += 1
        return(id_val)

    def get_site():
        return self.sites[self.counter] 

    def get_item(self, i_type = "none", passed_id = None):
        item = None
        if passed_id is not None:
            self.cur_id = passed_id
            item = self.addr_info[self.id_map[passed_id]]
        if self.cur_id not in id_map:
            item = self.addr_info[self.ipos]
            self.ipos += 1
        else:
            item = self.addr_info[self.id_map[self.cur_id]]
        if i_type == "none":
            ans = item
        elif i_type == "portfolio_id":
            ans = self.sites[self.counter]["portfolio_id"]
        elif i_type == "portfolio_name":
            ans = self.sites[self.counter]["portfolio_name"]
        elif i_type == "site_id":
            ans = self.sites[self.counter]["site_id"]
        elif i_type == "site_name":
            ans = self.sites[self.counter]["site_name"]
        elif i_type == "version":
            ans = self.version
        else:
            ans = item["address"][i_type]
        return item

def init_seed_data(conn):
    ans = {"addr_info": list(conn["sample_restaurants"]["restaurants"].find({},{"_id": 0, "address": 1, "borough": 1})),
           "sites" : generate_sites()
    }
    return ans

def generate_sites():
    port_ratio = settings["portfolios"]
    site_ratio = settings["sites"]
    batch_size = settings["batch_size"]
    batches = settings["batches"]
    num_to_do = int(batches * batch_size * site_ratio)
    ans = []
    for k in range(num_to_do):
        if k % 10 == 0:
            cur_portfolio_id = IDGEN.get("P-")
            cur_portfolio = fake.company()
        ans.append({
            "portfolio_id" : cur_portfolio_id,
            "portfolio_name" : cur_portfolio,
            "site_id" : IDGEN.get("S-"),
            "site_name" : fake.street_name()
        })

def synth_data_load():
    # python3 site_models.py action=load_data
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
    collection = settings["collection"]
    db = conn[settings["database"]]
    seed_data = init_seed_data(conn)
    CurInfo = CurItem({"version" : settings["version"], "addr_info" : seed_data["addr_info"], "sites" : seed_data["sites"]})
    #pprint.pprint(CurInfo.get_item("A-107"))
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
        base_counter = settings["base_counter"] + count * ipos
        IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefix})
        bb.message_box(f'[{pid}] {domain} - base: {base_counter}', "title")
        tot = 0
        batches = int(count/batch_size)
        if batches == 0:
            batches = 1
        for k in range(batches):
            #bb.logit(f"[{pid}] - {domain} Loading batch: {k} - size: {batch_size}")
            bulk_docs = build_batch_from_template(domain, {"connection" : conn, "template" : template_file, "batch" : k, "id_prefix" : prefix, "base_count" : base_counter, "size" : count, "cur_info": CurInfo})
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
    cur_info = None
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
    for J in range(0, batch_size): # iterate through the bulk insert count
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
                else:
                    islist = False
                    counts = random.randint(1, sub_size) #defaultdict(lambda: random.randint(1, 5))
                #print(f"# -- Procpath {path}")
                partial = procpath_new(path, counts, row[3]) # Note, later version of files may not include required field
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

def get_measurements(item_type = "chiller"):
    icnt = 10
    base_time = datetime.datetime.now() - datetime.timedelta(days = 1)
    arr = []
    for k in range(icnt):
        arr.append({
            "timestamp" : base_time + datetime.timedelta(seconds = 300),
            "temperature" : random.randint(60,80),
            "rotor_rpm" : random.randint(1200,3500),
            "input_temp" : random.randint(45,70),
            "output_temp" : random.randint(42,50),
            "output_pressure" : random.randint(60,110),
            "alarm" : "no"
        })
    return(arr)

#----------------------------------------------------------------------#
#   CSV Loader Routines
#----------------------------------------------------------------------#
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

def procpath_new(path, counts, generator):
    """Recursively walk a path, generating a partial tree with just this path's random contents"""
    stripped = stripProp(path[0])
    if len(path) == 1:
        # Base case. Generate a random value by running the Python expression in the text file
        #bb.logit(generator)
        return { stripped: eval(generator) }
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
            stripped: [ procpath_new([ path[0].replace(res,"") ] + path[1:], counts, generator)[stripped] for X in range(0, lcnt) ]
        }
    else:
        # Return a nested page, of the specified type, populated recursively.
        return {stripped: procpath_new(path[1:], counts, generator)}

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
    elif ARGS["action"] == "query":
        run_query()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()