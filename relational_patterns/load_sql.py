import sys
import os
import csv
#import vcf
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
import getopt
import bson
from bson.objectid import ObjectId
from bson.json_util import dumps
from bbutil import Util
from pymongo import MongoClient
import psycopg2
from faker import Faker
import itertools
from deepmerge import Merger
import uuid

fake = Faker()
letters = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
providers = ["cigna","aetna","anthem","bscbsma","kaiser"]

'''
 #  Relations Demo

  Providers
    provider
    provider_license
    provider_speciality
    provider_medicaid
    provider_hospitals
  Members
    member
    member_address
    member_communication
    member_guardian
    member_disability
    member_payment_methods
  Claims
    Claim_header
    Claim_line
    Payments

    python3 single_view.py action=load_mysql

# Startup Env:
    Atlas M10BasicAgain
    PostgreSQL
      export PATH="/usr/local/opt/postgresql@9.6/bin:$PATH"
      pg_ctl -D /usr/local/var/postgresql@9.6 start
      create database single_view with owner bbadmin;
      psql --username bbadmin single_view
'''
settings_file = "relations_settings.json"

class id_generator:
    def __init__(self, details = {}):
        self.tally = 100000
        if "seed" in details:
            self.tally = details["seed"]
    def set(self, seed):
        self.tally = seed
        return(self.tally)

    def get(self, prefix = "none"):
        if prefix == "none":
            prefix = random.choice(letters)
            prefix += random.choice(letters)
        result = f'{prefix}{self.tally}'
        self.tally += 1
        return result

def load_postgres_data():
    # read settings and echo back
    bb.message_box("Loading Data", "title")
    bb.logit(f'# Settings from: {settings_file}')
    ddl_action = "info"
    if "ddl" in ARGS:
        ddl_action = ARGS["ddl"]
    if "template" in ARGS:
        template = ARGS["template"]
    else:
        print("Send template=<pathToTemplate>")
        sys.exit(1)
    # Spawn processes
    num_procs = settings["process_count"]
    jobs = []
    inc = 0
    multiprocessing.set_start_method("fork", force=True)
    for item in range(num_procs):
        p = multiprocessing.Process(target=worker_load, args = (item, template))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def load_from_csv():
    # Provider.specialties().status,String,optional,"fake.random_element(('Active', 'Inactive'))","Active, Inactive",,,,Approved,,Provider.CredentialedSpecialties().Status,Embed-PegaHC-Stringlist,7.21
    boo = "boo"

def worker_load(ipos, template_name):
    #  Reads EMR sample file and finds values
    cur_process = multiprocessing.current_process()
    bb.message_box(f'({cur_process.name}) Loading Synth Data in SQL', "title")
    pgconn = pg_connection()
    IDGEN = id_generator()
    settings = bb.read_json(settings_file)
    batches = settings["batches"]
    #file_log(f'New process {cur_process.name}')
    start_time = datetime.datetime.now()
    for k in range(batches):
        bb.logit(f"Loading batch: {k}")
        result = build_sql_batch_from_template({"connection" : pgconn, "template" : template_name, "collection" : "notused"})

    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    pgconn.close()
    #file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def worker_customer_load(conn, tables):
    #  Reads EMR sample file and finds values
    table = 'provider_info'
    bulk_docs = []
    cur_process = multiprocessing.current_process()
    cnt = 0
    tot = 0

    for it in range(settings["num_records"]):
        age = random.randint(28,84)
        year = 2020 - age
        month = random.randint(1,12)
        day = random.randint(1,28)
        name = fake.name()
        id = IDGEN.get()
        doc = {}
        doc['member_id'] = id
        doc['benefit_plan_id'] = f'{random.choice(providers)}-{random.randint(20000,500000)}-{id}'
        #doc['birth_date'] = f"TIMESTAMP '{datetime.datetime(year,month,day, 10, 45).isoformat()}'"
        doc['birth_date'] = datetime.datetime(year,month,day, 10, 45)
        doc['first_name'] = name.split(" ")[0]
        doc['last_name'] = name.split(" ")[1]
        doc['phone'] = fake.phone_number()
        doc['email'] = f'{name.replace(" ",".")}@randomfirm.com'
        doc["gender"] = random.choice(["M","F"])
        doc["address1_type"] = "work"
        doc["address1_street"] = fake.street_address()
        doc["address1_line2"] = ""
        doc["address1_city"] = fake.city()
        doc["address1_state"] = fake.state_abbr()
        doc["address1_zipcode"] = fake.zipcode()
        doc["address2_type"] = "home"
        doc["address2_street"] = fake.street_address()
        doc["address2_line2"] = ""
        doc["address2_city"] = fake.city()
        doc["address2_state"] = fake.state_abbr()
        doc["address2_zipcode"] = fake.zipcode()
        bulk_docs.append(doc)
        MASTER_CUSTOMERS.append({"source" : "provider", "name" : name, "id" : id, "birth_date" : doc["birth_date"]})
        cnt += 1
        tot += 1
    record_loader(tables,table,bulk_docs,conn)
    bulk_docs = []
    cnt = 0
    bb.logit(f"{cur_process.name} Inserting Bulk, Total:{tot}")

def record_loader(tables, table, recs, nconn = False):
    # insert_into table fields () values ();
    if nconn:
        conn = nconn
    else:
        conn = pg_connection('postgres', tables[table]['database'])
    cur = conn.cursor()
    fields = list(recs[0])
    sql = tables[table]['insert']
    vals = []
    for record in recs:
        stg = list()
        for k in record:
            stg.append(record[k])
        vals.append(tuple(stg))
    print(sql)
    print(vals)
    try:
        cur.executemany(sql, vals)
        conn.commit()
        print(f'{cur.rowcount} inserted')
    except psycopg2.DatabaseError as err:
        bb.logit(f'{table} - {err}')
    cur.close()
    if not nconn:
        conn.close()

def value_codes(flds, special = {}):
    result = ""

    for i in range(len(flds)):
        fmt = "%s"
        if i in special:
            fmt = special[i]
        if i == 0:
            result = fmt
        else:
            result += f', {fmt}'
    return(result)


def increment_version(old_ver):
    parts = old_ver.split(".")
    return(f'{parts[0]}.{int(parts[1]) + 1}')

#----------------------------------------------------------------------#
#   CSV Loader Routines
#----------------------------------------------------------------------#
stripProp = lambda str: re.sub(r'\s+', '', (str[0].upper() + str[1:].strip('()')))

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

def zipmerge(the_merger, path, base, nxt):
    """Strategy for deepmerge that will zip merge two lists. Assumes lists of equal length."""
    return [ the_merger.merge(base[i], nxt[i]) for i in range(0, len(base)) ]

def ID(key):
    id_map[key] += 1
    return key + str(id_map[key]+base_counter)

def local_geo():
    coords = fake.local_latlng('US', True)
    return coords

#-----------------------------------------------------------#
#  CSV template sql translator
#-----------------------------------------------------------#
def build_sql_batch_from_template(details = {}):
    template_file = details["template"]
    collection = details["collection"]
    batch_size = settings["batch_size"]
    cnt = 0
    bulk = []
    master_table = template_file.split("/")[-1].split(".")[0]
    tables = ddl_from_template(template_file, master_table)
    bb.logit(f'Master: {master_table} - Bulk docs[{batch_size}]')
    master_id = f'{master_table}_id'
    master_ids = []
    g_id = ""
    database = "none"
    data = {}
    for item in tables:
        attrs = tables[item]
        cur_table = item
        is_master = True if cur_table == master_table else False
        recs = []
        bb.logit(f'Table: {cur_table} building data')
        database = attrs["database"]
        counts = random.randint(1, 5) if len(cur_table.split("_")) > 1 else 1
        bb.logit(f'Table: {cur_table} building data, factor: {counts}')
        sql = attrs["insert"]
        idpos = 0
        for inc in range(0, batch_size * counts): # iterate through the bulk insert count
            fld_cnt = 0
            hsh = {}
            if idpos > batch_size - 1:
                idpos = 0
            for cur_field in attrs["fields"]:
                #bb.logit(f'Field: {cur_field} - gen {attrs["generator"][fld_cnt]}')
                if is_master and cur_field == master_id:
                    g_id = eval(attrs["generator"][fld_cnt])
                    cur_val = g_id
                    master_ids.append(g_id)
                    bb.logit(f'[{cnt}] - GlobalID = {g_id}')
                    #is_master = False
                    fld_cnt += 1
                elif cur_field == master_id:
                    bb.logit(f'IDPOS: {idpos}')
                    cur_val = master_ids[idpos]
                else:
                    cur_val = eval(attrs["generator"][fld_cnt])
                    if cur_val == True:
                        cur_val = "T"
                    elif cur_val == False:
                        cur_val = "F"
                    fld_cnt += 1
                hsh[cur_field] = cur_val
            idpos += 1
            cnt += 1
            recs.append(hsh)
        record_loader(tables,cur_table,recs,details["connection"])
        bb.logit(f'{batch_size} {cur_table} batch complete (tot = {cnt})')
    bb.logit(f'{cnt} records for {database} complete')
    return(bulk)

def ddl_from_template(action = "none", pgconn = "none"):
    database = settings["postgres"]["database"]
    if "template" in ARGS:
        template = ARGS["template"]
    else:
        print("Send template=<pathToTemplate>")
        sys.exit(1)
    action = "none"
    if "task" in ARGS:
        action = ARGS["task"]
    bb.message_box("Generating DDL")
    # Read the csv file and digest
    fields = fields_from_template(template)
    #pprint.pprint(fields)
    tables = {}
    last_table = "zzzzz"
    ddl = ""
    for row in fields:
        table, field, ftype = clean_field_data(row)
        if table not in tables:
            #bb.logit("#--------------------------------------#")
            bb.logit(f'Building table: {table}')
            last_table = table
            fkey = ""
            flds = [field]
            if len(table.split("_")) == 2:
                #  Add a parent_id field
                new_field = f'{row["parent"]}_id'
                fkey = f'  {new_field} varchar(20) NOT NULL,'
                flds.append(new_field)
            ddl = (
                f'CREATE TABLE {table} ('
                "  id SERIAL PRIMARY KEY,"
                f'{fkey}'
                f'  {field} {ftype},'
            )
            tables[table] = {"ddl" : ddl, "database" : database, "fields" : flds, "generator" : [row["generator"]]}

        else:
            #bb.logit(f'Adding table data: {table}, {field}')
            if field not in tables[table]["fields"]:
                tables[table]["ddl"] = tables[table]["ddl"] + f'  {field} {ftype},'
                tables[table]["fields"].append(field)
                tables[table]["generator"].append(row["generator"])
        first = False
    clean_ddl(tables)
    bb.logit("Table DDL:")
    pprint.pprint(tables)
    sql_action(pgconn, action, tables)
    return(tables)

def clean_field_data(data):
    tab = data["table"]
    if data["name"].lower() == "id":
        data["name"] = f'{tab}_id'
    if len(tab.split("_")) == 2 and tab.split("_")[0] == tab.split("_")[1]: #"catch doubled eg member_member"
        tab = tab.split("_")[0]
    return tab, data["name"], data["type"]

def clean_ddl(tables_obj):
    for tab in tables_obj:
        ddl = tables_obj[tab]["ddl"]
        l = len(ddl)
        ddl = ddl[:l-1] + ")"
        tables_obj[tab]["ddl"] = ddl
        fmts = value_codes(tables_obj[tab]["fields"])
        tables_obj[tab]["insert"] = f'insert into {tab} ({",".join(tables_obj[tab]["fields"])}) values ({fmts});'

def pg_type(mtype):
    type_x = {"string" : "varchar(100)","boolean" : "varchar(2)","date" : "timestamp", "integer" : "integer","double" : "real"}
    ftype = type_x[mtype.lower().strip()]
    return ftype

def fields_from_template(template):
    '''
    {'name': 'EffectiveDate', 'table': 'member_address', 'type': 'date', 'generator' : "fake.date()", 'parent' : 'member'},
    '''
    ddl = []
    with open(template) as csvfile:
        propreader = csv.reader(itertools.islice(csvfile, 1, None))
        master = ""
        # support for parent.child.child.field, parent.children().field
        for row in propreader:
            result = {"name" : "","table" : "", "parent" : "", "type" : pg_type(row[1]),"generator": row[3]}
            path = row[0].split('.')
            depth = len(path)
            for k in range(depth):
                path[k] = path[k].strip("()").strip()
            result["name"] = path[-1]
            master = f'{path[0]}'
            if depth == 1:
                bb = "" # should never see this
            elif depth == 2:
                result["table"] = master
            elif depth == 3:
                result["table"] = f'{path[0]}_{path[1]}'
                result["parent"] = master
            elif depth == 4:
                result["table"] = f'{path[1]}_{path[2]}'
                result["parent"] = f'{path[0]}_{path[1]}'
            elif depth == 5:
                result["table"] = f'{path[2]}_{path[3]}'
                result["parent"] = f'{path[1]}_{path[2]}'
            ddl.append(result)
    return(ddl)

#----------------------------------------------------------------------#
#   Utility Routines
#----------------------------------------------------------------------#
def sql_action(conn, action, tables):
    if action == "none":
        return("no action")
    sql = ""
    if action == "create":
        cursor = conn.cursor()
        for table_name in tables:
            if action == "create":
                sql = tables[table_name]['ddl']
            elif action == "drop":
                sql = f'DROP TABLE {table_name};'
            elif action == "delete":
                sql = f'delete from {table_name};'
            try:
                bb.logit(f"Action: {action} {table_name}")
                print(sql)
                cursor.execute(sql)
            except psycopg2.DatabaseError as err:
                bb.logit(pprint.pformat(err))
            else:
                print("OK")
                conn.commit()
        cursor.close()
    return("success")

def pg_connection(type = "postgres", sdb = 'none'):
    # cur = mydb.cursor()
    # cur.execute("select * from Customer")
    # result = cursor.fetchall()
    shost = settings[type]["host"]
    susername = settings[type]["username"]
    spwd = settings[type]["password"]
    if sdb == 'none':
        sdb = settings[type]["database"]
    conn = psycopg2.connect(
        host = shost,
        database = sdb,
        user = susername,
        password = spwd
    )
    return conn

#------------------------------------------------------------------#
#     MAIN
#------------------------------------------------------------------#
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    base_counter = settings["base_counter"]
    IDGEN = id_generator({"seed" : base_counter})
    id_map = defaultdict(int)
    MASTER_CUSTOMERS = []
    if "wait" in ARGS:
        interval = int(ARGS["wait"])
        if interval > 10:
            bb.logit(f'Delay start, waiting: {interval} seconds')
            time.sleep(interval)
    #conn = client_connection()
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "load_pg_data":
        load_postgres_data()
    elif ARGS["action"] == "notexecute_ddl":
        mycon = pg_connection()
        execute_ddl('create', mycon)
        mycon.close
    elif ARGS["action"] == "test_csv":
        result = build_batch_from_template({"template" : "model-tables/member.csv", "collection" : "notused", "batch_size" : 4})
        pprint.pprint(result)
    elif ARGS["action"] == "execute_ddl":
        mycon = pg_connection()
        ddl_from_template("create", mycon)
        mycon.close
    elif ARGS["action"] == "reset_data":
        reset_data()
    elif ARGS["action"] == "microservice":
        microservice_one()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()
