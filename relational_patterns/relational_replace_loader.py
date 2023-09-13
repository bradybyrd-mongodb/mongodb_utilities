import sys
import os
import csv
#import vcf
from collections import OrderedDict
from collections import defaultdict
from deepmerge import Merger
import itertools
import json
import random
import time
import re
import multiprocessing
import pprint
import bson
from bson.objectid import ObjectId
from decimal import Decimal
from bbutil import Util
from id_generator import Id_generator
import datetime
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
from pymongo import UpdateMany
from faker import Faker

fake = Faker()

'''
  Relations Loader
  4/25/22 BJB
Using the Members/Providers/Claims model, walk through relational patterns from SQL to MongoDB

One-One => Simple
One-Many => Embed, PartialEmbed
Many-Many => Assymetric embedding

'''
settings_file = "relations_settings.json"

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
    collection = settings["collection"]
    db = conn[settings["database"]]
    #IDGEN = Id_generator({"seed" : base_counter, "size" : count})
    bulk_docs = []
    ts_start = datetime.datetime.now()
    cur_time = ts_start
    cnt = 0
    tot = 0
    if "template" in args:
        template = args["template"]
        master_table = master_from_file(template)
        job_info = {master_table : {"path" : template, "size" : settings["batches"] * settings["batch_size"], "id_prefix" : f'{master_table[0].upper()}-'}}
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
        counts = random.randint(1, 5) #defaultdict(lambda: random.randint(1, 5))
        data = {}
        with open(template_file) as csvfile:
            propreader = csv.reader(csvfile)
            icnt = 0
            for row in propreader:
                islist = "n"
                if icnt == 0:
                    icnt += 1
                    continue
                #print(row)
                path = row[0].split('.')
                if "()" in row[0]: #path[-2].endswith('()'):
                    islist = "Y"
                else:
                    counts = random.randint(1, 5) #defaultdict(lambda: random.randint(1, 5))
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
        data["version"] = settings["version"]
        cnt += 1
        records.append(data)
    #bb.logit(f'{batch_size} {cur_coll} batch complete')
    return(records)

def master_from_file(file_name):
    return file_name.split("/")[-1].split(".")[0]

def check_file(type = "delete"):
    #  file loader.ctl
    ctl_file = "loader.ctl"
    result = True
    with open(ctl_file, 'w', newline='') as controlfile:
        status = controlfile.read()
        if "stop" in status:
            result = False
    return(result)

def synth_data_update():
    # read settings and echo back
    #  python3 vcf_loader.py action=emr_data
    multiprocessing.set_start_method("fork", force=True)
    bb.message_box("Updating Data", "title")
    start_time = datetime.datetime.now()
    bb.logit(f'# Settings from: {settings_file}')
    # Spawn processes
    num_procs = settings["process_count"]
    jobs = []
    inc = 0
    for item in range(num_procs):
        p = multiprocessing.Process(target=worker_updater, args = (item,))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    bb.logit(f"{main_process.name} - Execution total took {execution_time} seconds")

def worker_updater(ipos):
    #  Updates documents and finds values
    cur_process = multiprocessing.current_process()
    pid = cur_process.pid
    conn = client_connection()
    bb.message_box(f"[{pid}] Worker Data", "title")
    settings = bb.read_json(settings_file)
    base_counter = settings["base_counter"]
    procs = settings["process_count"]
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    start_time = datetime.datetime.now()
    db = settings["database"]
    job_info = settings["data"]
    # Loop through collection files
    for domain in job_info:
        details = job_info[domain]
        if "thumbnail" in details:
            prefix = details["id_prefix"]
            count = details["size"]
            low_id = base_counter + (count * ipos)
            high_id = base_counter + (count * (ipos + 1))
            criteria = {"$and" : [{f'{domain}_id' : {"$gt" : f'{prefix}{low_id}'}},{f'{domain}_id' : {"$lte" : f'{prefix}{high_id}'}}]}
            tasks = details["thumbnail"]
            bb.message_box(f'[{pid}] {domain}', "title")
            for item in tasks:
                if item["type"] == "many":
                    update_related_many(domain,conn[db],item,criteria)
                else:
                    update_related_one(domain,conn[db],item,criteria)
            tot = 0
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    conn.close()
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def update_related_one(domain, db, update_info, crit):
    bb.logit(f'UpdatingOne: {update_info}, crit: {crit}')
    cursor = db[domain].find(crit)
    interval = 10
    inc = 0
    for doc in cursor:
        cur_qry = eval(update_info["find_query"])
        adoc = db[update_info["coll"]].find_one(cur_qry)
        if adoc is not None:
            newdoc = {}
            #bb.message_box("DOC")
            #pprint.pprint(doc)
            #bb.message_box(f"ADOC - {cur_qry}")
            #pprint.pprint(adoc)
            for item in update_info["fields"]:
                res = adoc
                pnam = ""
                for level in item.split("."):
                    if level.isdigit():
                        level = int(level)
                    else:
                        pnam += level
                    res = res[level]

                newdoc[pnam] = res
            db[domain].update_one({"_id": doc["_id"]},{"$set": {update_info["name"] : newdoc}})
        if inc % interval == 0:
            bb.logit(f"UpdatingOne: {inc} completed")
        inc += 1
    bb.logit(f"All done - {inc} completed")

def update_related_many(domain, db, update_info, crit):
    bb.logit(f'UpdatingMany: {update_info}, crit: {crit}')
    cursor = db[domain].find(crit)
    interval = 10
    inc = 0
    for doc in cursor:
        many_crit = eval(update_info["find_query"])
        cntq = db[update_info["coll"]].count_documents(many_crit)
        cur2 = db[update_info["coll"]].find(many_crit)
        newd = []
        for adoc in cur2:
            newdoc = {}
            for item in update_info["fields"]:
                res = adoc
                pnam = ""
                for level in item.split("."):
                    if level.isdigit():
                        level = int(level)
                    else:
                        pnam += level
                    res = res[level]
                newdoc[pnam] = res
            newd.append(newdoc)
        db[domain].update_one({"_id": doc["_id"]},{"$set": {update_info["name"] : newd}})
        if inc % interval == 0:
            bb.logit(f'UpdatingMany: {inc} completed - many: db.{update_info["coll"]}.find({many_crit}), cnt: {cntq}')
        inc += 1
    bb.logit(f"All done - {inc} completed")

def add_primary_provider_ids():
    num_provs = 200
    base_val = 1000000
    query = {}
    conn = client_connection()
    bb.message_box(f" Updater", "title")
    db = conn[settings["database"]]
    recs = db["member"].find(query)
    for item in recs:
        #print(f'item: {item}')
        #pid = f'P-{random.randint(base_val, base_val + num_provs)}'
        if "primaryProvider_id" in item:
            pid = item["primaryProvider_id"]
            prov = db["provider"].find_one({"provider_id" : pid})
            if prov is not None:
                prov_doc = {"primaryProvider" : {"provider_id" : pid, "nationalProviderIdentifier" : prov["nationalProviderIdentifier"], "firstName" : prov["firstName"], "lastName": prov["lastName"], "dateOfBirth": prov["dateOfBirth"], "gender" : prov["gender"]}}
                db["member"].update_one({"_id" : item["_id"]},{"$set" : prov_doc, "$unset" : {"primaryProvider_id": ""}})
        #print(sql)
        bb.logit(f'Update: {item["member_id"]}')

def update_phi():
    # Adds phi fields and client_id for redaction scenario
    num_provs = 200
    base_val = 1000000
    query = {}
    conn = client_connection()
    bb.message_box(f" PHI Updater", "title")
    db = conn[settings["database"]]
    recs = db["member"].find(query)
    for item in recs:
        #print(f'item: {item}')
        #pid = f'P-{random.randint(base_val, base_val + num_provs)}'
        if "primaryProvider_id" in item:
            pid = item["primaryProvider_id"]
            prov = db["provider"].find_one({"provider_id" : pid})
            if prov is not None:
                prov_doc = {"primaryProvider" : {"provider_id" : pid, "nationalProviderIdentifier" : prov["nationalProviderIdentifier"], "firstName" : prov["firstName"], "lastName": prov["lastName"], "dateOfBirth": prov["dateOfBirth"], "gender" : prov["gender"]}}
                db["member"].update_one({"_id" : item["_id"]},{"$set" : prov_doc, "$unset" : {"primaryProvider_id": ""}})
        #print(sql)
        bb.logit(f'Update: {item["member_id"]}')

def update_member_ids():
    bb.message_box("MemberID alignment","title")
    conn = client_connection()
    bb.message_box(f" PHI Updater", "title")
    db = conn[settings["database"]]
    batch_size = 2000
    member_ratio = 10
    totdocs = 350000
    num_batches = int(totdocs/(batch_size * member_ratio))
    pipe = [
        {"$sample" : {"size" : batch_size * member_ratio}},
        {"$project": {"patient_id": 1}}
    ]
    inc = 0
    settings["batch_size"] = batch_size
    m_ids = []
    get_list = 0
    g_cnt = 0
    tcnt = 0
    for curbatch in range(num_batches):
        if get_list == 0 or get_list == member_ratio - 1:
            bb.logit(f'Getting {batch_size * member_ratio} claim_ids')
            claims = list(db["claim_phi"].aggregate(pipe))
            m_ids = []
            get_list = 0    
        recs = build_batch_from_template("member", {"path" : "model-tables/member_phi.csv"})
        for icnt in range(batch_size):
            try:
                recs[icnt]["member_id"] = claims[icnt]["patient_id"]
                recs[icnt]["version"] = "1.5"
                m_ids.append(claims[icnt]["patient_id"])
                tcnt += 1
            except Exception as e:
                print(f'ERROR - {icnt}, recs: {len(recs)}')
                print(e)
                exit(1)
        db["member"].insert_many(recs)
        bb.logit(f"Saving batch (member) [{batch_size}] - total: {tcnt}")
        
        bulk_updates = []
        totcnt = 0
        for k in range(member_ratio):
            for icnt in range(batch_size):
                bulk_updates.append(
                    UpdateOne({"_id" : claims[totcnt]["_id"]},{"$set": {"patient_id": m_ids[icnt]}})
                )
                totcnt += 1
            if k > 0:
                bulk_writer(db["claim_phi"], bulk_updates)
                bb.logit(f'Updating claim-member_id [{batch_size}] - total: {totcnt}, grandTotal: {g_cnt}')
            bulk_updates = []
        get_list += 1
        g_cnt += totcnt
    bb.logit(f"All done - {inc} completed")

def update_member_thumbnail():
    bb.message_box("MemberID de-norm","title")
    conn = client_connection()
    db = conn[settings["database"]]
    batch_size = 2000
    batches = 5
    pipe = [
        {"$match" : {"version" : "1.6"}},
        {"$sample" : {"size" : batch_size}}
    ]
    inc = 0
    tcnt = 0
    bulk_updates = []
    for k in range(batches):
        members = db["member"].aggregate(pipe)
        for curdoc in members:
            #pprint.pprint(curdoc)
            newdoc = {"member_id" : curdoc["member_id"], "firstName" : curdoc["phi"]["firstName"], "lastName" : curdoc["phi"]["lastName"], "birthDate" : curdoc["phi"]["dateOfBirth"], "gender": curdoc["gender"]}
            bulk_updates.append(
                UpdateMany({"patient_id" : curdoc["member_id"]},{"$set": {"patient": newdoc}})
            )
            tcnt += 1
        
        bb.logit(f"Saving batch (member) [{batch_size}] - total: {tcnt}")
        bulk_writer(db["claim_phi"], bulk_updates)
        bulk_updates = []
    bb.logit(f"All done - {inc} completed")

def update_birthdate():
    bb.message_box("Birthday alignment","title")
    conn = client_connection()
    db = conn[settings["database"]]
    batch_size = 2000
    settings["batch_size"] = batch_size
    peeps = list(db["member"].find({"version": "1.6"},{"phi.dateOfBirth": 1}))
    bulk_updates = []
    numtodo = len(peeps)
    for icnt in range(numtodo - 1):
        year = 2023 - random.randint(16,87)
        month = random.randint(1,12)
        day = random.randint(1,28)
        new_date = datetime.datetime(year,month,day, 10, 45)
        bulk_updates.append(
            UpdateOne({"_id" : peeps[icnt]["_id"]},{"$set": {"phi.dateOfBirth": new_date}})
        )
        if icnt > 0 and icnt % batch_size == 0:
            bulk_writer(db["member"], bulk_updates)
            bb.logit(f'Updating member birthday [{batch_size}] - total: {icnt}')
            bulk_updates = []

    bb.logit(f"All done - {numtodo} completed")

#----------------------------------------------------------------------#
#   CSV Loader Routines
#----------------------------------------------------------------------#
#stripProp = lambda str: re.sub(r'\s+', '', (str[0].lower() + str[1:].strip('()')))
def stripProp(str):
    ans = str
    if str[0].isupper() and str[1].islower():
        ans = str[0].lower() + str[1:]
    ans = re.sub(r'\s+', '', ans.strip('()'))
    return ans

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
        print("# ------------------------------------------ #")
        print(f'{stripped} - {counts} - {generator}')
        return {            
            stripped: [ procpath([ path[0].strip('()') ] + path[1:], counts, generator)[stripped] for X in range(0, counts[stripped]) ]
        }
    else:
        # Return a nested page, of the specified type, populated recursively.
        return {stripped: procpath(path[1:], counts, generator)}

def procpath_new(path, counts, generator):
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
            stripped: [ procpath_new([ path[0].strip('()') ] + path[1:], counts, generator)[stripped] for X in range(0, counts) ]
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

def fix_member_id():
    conn = client_connection()
    db = conn["healthcare"]
    inc = 0
    cursor = db["member"].find({})
    for adoc in cursor:
        idnum = int(adoc["Member_id"].split("-")[1]) - 50
        new_id = f'M-{idnum}'
        db["member"].update_one({"_id": adoc["_id"]},{"$set": {"Member_id" : new_id}})
        bb.logit(f"Updating: {inc} completed")
        inc += 1

def id_manager(pnum, args):
    cur_process = multiprocessing.current_process()
    module = "IDGEN"
    prefix = module[0].upper() + "-"
    id = f'{prefix}|{cur_process.pid}'
    queues = args["queues"]
    iters = settings["iters"]
    base_counter = settings["base_counter"]
    bb.message_box(f"[{cur_process.pid}] {module}", "title")
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    IDGEN = Id_generator({"seed" : base_counter})
    doit = True
    while doit:
        for conn in queues:
            pip = conn[1]
            req = pip.recv()
            bb.logit(f'{module} - {req}')
            if req[0] == "stop":
                doit = False
            elif req[1] == "pass":
                doit = True
            elif req[1] == "get":
                # [id, "get", prefix, iters]
                ans = IDGEN.get(req[2], req[3])
                req = pip.send([req[0], "response", ans])
            #time.sleep(.05)

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
    cur_process = multiprocessing.current_process()
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    bb.logit("Performing 5000 queries")
    conn = client_connection()
    db = conn[settings["database"]]
    num = len(cc.lexicon)
    cnt = 0
    for k in range(int(5000/num)):
        for term in cc.lexicon:
            start = datetime.datetime.now()
            output = db.emr.find({"disease" : {"$regex" : f'^{term}.*'}}).count()
            if cnt % 100 == 0:
                end = datetime.datetime.now()
                elapsed = end - start
                secs = (elapsed.seconds) + elapsed.microseconds * .000001
                bb.logit(f"{cur_process.name} - Query: Disease: {term} - Elapsed: {format(secs,'f')} recs: {output} - cnt: {cnt}")
            cnt += 1
            #time.sleep(.5)

def client_connection(type = "uri", details = {}):
    mdb_conn = settings[type]
    username = settings["username"]
    password = settings["password"]
    if "secret" in password:
        password = os.environ.get("PWD")
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
    elif ARGS["action"] == "update_relations":
        synth_data_update()
    elif ARGS["action"] == "update_member_ids":
        update_member_ids()
    elif ARGS["action"] == "update_members":
        update_member_thumbnail()
    elif ARGS["action"] == "update_provider":
        add_primary_provider_ids()
    elif ARGS["action"] == "update_birthdate":
        update_birthdate()
    elif ARGS["action"] == "update_phi":
        add_phi_data()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()
