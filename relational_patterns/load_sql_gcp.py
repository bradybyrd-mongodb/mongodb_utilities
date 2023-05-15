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
import getopt
import bson
from bson.objectid import ObjectId
from bson.json_util import dumps
from bbutil import Util
from id_generator import Id_generator
from pymongo import MongoClient
#import psycopg2
from faker import Faker
import itertools
from deepmerge import Merger
import uuid

fake = Faker()
letters = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
providers = ["cigna","aetna","anthem","bscbsma","kaiser"]

#  BJB 5/15/23 Big Query
from google.cloud import bigquery


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


def load_bigquery_data():
    # read settings and echo back
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
        sys.exit(1)
    execute_ddl()
    # Spawn processes
    num_procs = settings["process_count"]
    jobs = []
    inc = 0
    multiprocessing.set_start_method("fork", force=True)
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

def load_from_csv():
    # Provider.specialties().status,String,optional,"fake.random_element(('Active', 'Inactive'))","Active, Inactive",,,,Approved,,Provider.CredentialedSpecialties().Status,Embed-PegaHC-Stringlist,7.21
    boo = "boo"

def worker_load(ipos, args):
    #  Reads EMR sample file and finds values
    cur_process = multiprocessing.current_process()
    bb.message_box(f'({cur_process.name}) Loading Synth Data in SQL', "title")
    pgconn = pg_connection()
    settings = bb.read_json(settings_file)
    batches = settings["batches"]
    batch_size = settings["batch_size"]
    if "template" in args:
        template = args["template"]
        master_table = master_from_file(template)
        job_info = {master_table : {"path" : template, "size" : settings["batches"] * settings["batch_size"]}, "id_prefix" : f'{master_table[0].upper()}-'}
    else:
        job_info = settings["data"]
    start_time = datetime.datetime.now()
    #IDGEN = Id_generator({"seed" : base_counter, "size" : details["size"]})
    for domain in job_info:
        details = job_info[domain]
        template_file = details["path"]
        count = details["size"]
        prefix = details["id_prefix"]
        base_counter = settings["base_counter"] + count * ipos
        bb.message_box(domain, "title")
        table_info = ddl_from_template("none", pgconn, template_file, domain)
        batches = int(details["size"]/batch_size)
        IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefix})
        for k in range(batches):
            bb.logit(f"Loading batch: {k} - size: {batch_size}")
            result = build_sql_batch_from_template(table_info, {"master" : domain, "connection" : pgconn, "template" : template_file, "batch" : k, "id_prefix" : prefix, "base_count" : base_counter, "size" : count})

    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    pgconn.close()
    #file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

#-----------------------------------------------------------#
#  CSV template sql translator
#-----------------------------------------------------------#
def build_sql_batch_from_template(tables, details = {}):
    template_file = details["template"]
    batch_size = settings["batch_size"]
    base_counter = details["base_count"]
    num_procs = settings["process_count"]
    batch = details["batch"]
    master_table = details["master"]
    master_id = f'{master_table}_id'.lower()
    cnt = 0
    tab_types = table_types(tables)
    bb.logit(f'Master: {master_table} - building: {batch_size}')
    master_ids = []
    rec_counts = {}
    g_id = ""
    database = "none"
    data = {}
    for item in tables:
        attrs = tables[item]
        cur_table = item
        parent = attrs["parent"]
        table_type = tab_types[cur_table]
        recs = []
        bb.logit(f'Table: {cur_table} building data')
        database = attrs["database"]
        if table_type == "submaster":
            prefx = cur_table[0:2].upper() + "-"
            count = details["size"] * num_procs
            IDGEN.set({"seed" : base_counter, "size" : count, "prefix" : prefx})
        elif table_type == "none" and len(parent.split("_")) > 1:
            id_prefix = parent[0:2].upper() + "-"
        else:
            id_prefix = details["id_prefix"]
        counts = random.randint(1, 5) if len(cur_table.split("_")) > 1 else 1
        bb.logit(f'Table: {cur_table} building data, factor: {counts}')
        sql = attrs["insert"]
        idpos = 0
        rec_counts[cur_table] = batch_size * counts * num_procs
        for inc in range(0, batch_size * counts): # iterate through the bulk insert count
            fld_cnt = 0
            hsh = {}
            if idpos > batch_size - 1:
                idpos = 0
            for cur_field in attrs["fields"]:
                #bb.logit(f'Field: {cur_field} - gen {attrs["generator"][fld_cnt]}')
                if table_type == "master" and cur_field.lower() == master_id:
                    # master e.g. claim_id
                    g_id = eval(attrs["generator"][fld_cnt])
                    cur_val = g_id
                    master_ids.append(g_id)
                    #bb.logit(f'[{cnt}] - GlobalID = {g_id}')
                    #is_master = False
                    fld_cnt += 1
                elif cur_field.lower() == master_id:
                    #bb.logit(f'IDPOS: {idpos}')
                    cur_val = master_ids[idpos]
                elif cur_field.lower().replace("_id","") == attrs["parent"].lower():
                    # child of e.g. claim_claimline.claim_id
                    cur_val = IDGEN.random_value(id_prefix)
                    #bb.logit(f'IDsub[{cur_val}] {cur_table} - {attrs["parent"]}\n{IDGEN.value_history}')
                elif cur_field.lower() == f'{cur_table.lower()}_id':
                    #Internal id for table
                    prefx = cur_table[0:2].upper() + "-"
                    if table_type == "submaster":
                        cur_val = IDGEN.get(prefx)
                    else:
                        cur_val = f'{prefx}{random.randint(1000,1000000)}'
                else:
                    cur_val = eval(attrs["generator"][fld_cnt])
                    if type(cur_val) is bool:
                        cur_val = 'T' if cur_val == True else "F"
                    fld_cnt += 1
                hsh[cur_field.lower()] = cur_val
            idpos += 1
            cnt += 1
            recs.append(hsh)
        record_loader(tables,cur_table,recs,details["connection"])
        bb.logit(f'{batch_size} {cur_table} batch complete (tot = {cnt})')
    bb.logit(f'{cnt} records for {database} complete')
    return cnt

def table_types(table_info):
    res = {}
    subs = []
    for tab in table_info:
        if table_info[tab]["parent"] not in subs:
            subs.append(table_info[tab]["parent"])
            
    for tab in table_info:
        res[tab] = "none"
        attrs = table_info[tab]
        if attrs["parent"] == "":
            res[tab] = "master"
        elif tab in subs:
            res[tab] = "submaster"
    return res

def ddl_from_template(action, pgconn, template, domain):
    database = settings["postgres"]["database"]
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
            flds = []
            schema = []

            if len(table.split("_")) > 1:
                #  Add a parent_id field
                field_name = f'{row["parent"]}_id'
                new_field = bigquery.SchemaField(field_name, "STRING", mode="REQUIRED")
                flds.append(field_name)
                schema.append(new_field)
                #  Add a self_id field
                field_name = f'{table}_id'
                new_field = bigquery.SchemaField(field_name, "STRING", mode="REQUIRED")
                flds.append(field_name)
                schema.append(new_field)
            table_id = biquery.Table.from_string(f'{gcp_project_id}.{gcp_dataset}.{table}')

            tables[table] = {"table_id" : table_id, "schema" : schema, "database" : database, "fields" : flds, "generator" : [row["generator"]], "parent" : row["parent"]}

        else:
            #bb.logit(f'Adding table data: {table}, {field}')
            if field not in tables[table]["fields"]:
                new_field = bigquery.SchemaField(field, ftype)
                tables[table]["schema"].append(new_field)
                tables[table]["fields"].append(field)
                tables[table]["generator"].append(row["generator"])
        first = False
    clean_ddl(tables)
    bb.logit("Table DDL:")
    pprint.pprint(tables)
    sql_action(pgconn, action, tables)
    return(tables)

def master_from_file(file_name):
    return file_name.split("/")[-1].split(".")[0]

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

def gcp_type(mtype):
    type_x = {"string" : "STRING","boolean" : "BOOL","date" : "DATETIME", "integer" : "INT64","double" : "FLOAT64"}
    ftype = type_x[mtype.lower().strip()]
    return ftype

def generator_values(gen):
    # substitute params for generic generator values
    return gen

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
            result = {"name" : "","table" : "", "parent" : "", "type" : gcp_type(row[1]),"generator": generator_values(row[3])}
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
                result["table"] = f'{path[0]}_{path[1]}_{path[2]}'
                result["parent"] = f'{path[0]}_{path[1]}'
            elif depth == 5:
                result["table"] = f'{path[0]}_{path[1]}_{path[2]}_{path[3]}'
                result["parent"] = f'{path[0]}_{path[1]}_{path[2]}'
            ddl.append(result)
    return(ddl)

def execute_ddl(ddl_action = "info"):
    if "template" in ARGS:
        template = ARGS["template"]
    elif "data" in settings:
        goodtogo = True
    else:
        print("Send template=<pathToTemplate>")
        sys.exit(1)
    if "task" in ARGS:
        ddl_action = ARGS["task"]
    mycon = bigquery_connection()
    if "template" in ARGS:
        master_table = master_from_file(template)
        job_info = {master_table : {"path" : template, "size" : settings["batches"] * settings["batch_size"], "id_prefix" : f'{master_table[0].upper()}-'}}
    else:
        job_info = settings["data"]
    start_time = datetime.datetime.now()
    for domain in job_info:
        details = job_info[domain]
        bb.message_box(domain, "title")
        bb.logit(details["path"])
        template_file = details["path"]
        table_info = ddl_from_template(ddl_action, mycon, template_file, domain)
    mycon.close

def create_foreign_keys():
    #  Reads settings file and finds values
    cur_process = multiprocessing.current_process()
    bb.message_box(f'({cur_process.name}) Creating Foreign Keys in SQL', "title")
    start_time = datetime.datetime.now()
    pgconn = pg_connection()
    settings = bb.read_json(settings_file)
    cur = pgconn.cursor()
    cur2 = pgconn.cursor()
    sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    cur.execute(sql)
    for item in cur:
        bb.logit(f'item: {item}')
        try:
            fkey_sql = foreign_key_sql(item[0])
            if fkey_sql != "none":
                print(fkey_sql)
                cur2.execute(fkey_sql)
                pgconn.commit()
        except psycopg2.DatabaseError as err:
            bb.logit(f'{err}', "ERROR")
            pgconn.commit()
            #cur2.close()
            #cur2 = pgconn.cursor()
    cur.close()
    cur2.close()
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    pgconn.close()
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def foreign_key_sql(table):
    parts = table.split("_")
    part_size = len(parts)
    child = parts[-1]
    if part_size == 1:
        return("none")
    elif part_size == 2:
        parent = parts[0]
    elif part_size == 3:
        parent = f'{parts[0]}_{parts[1]}'
    fkey = f'{parent}_id'
    sql = (f"ALTER TABLE IF EXISTS public.{table}\n"
        f"ADD CONSTRAINT fky_{fkey} FOREIGN KEY ({fkey})\n"
        f"REFERENCES public.{parent} ({fkey}) MATCH SIMPLE\n"
        f"ON UPDATE NO ACTION\n"
        f"ON DELETE NO ACTION\n"
        f" NOT VALID")
    return(sql)

def fix_provider_ids():
    num_provs = 50
    base_val = 1000000
    query_sql = "select id from claim_claimline"
    rsql = "SELECT floor(random()*(1000050-1000000+1))+1000000"
    mycon = pg_connection()
    cur = mycon.cursor()
    cur2 = mycon.cursor()
    try:
        cur.execute(query_sql)
        for item in cur:
            print(f'item: {item}')
            pid = f'P-{random.randint(base_val, base_val + num_provs)}'
            rpid = f'P-{random.randint(base_val, base_val + num_provs)}'
            sql = f'update claim_claimline set attendingprovider_id = \'{pid}\', operatingprovider_id = \'{pid}\', '
            sql += f' orderingprovider_id = \'{rpid}\',  referringprovider_id = \'{rpid}\' '
            sql += f"where id = {item[0]}"
            #print(sql)
            cur2.execute(sql)
            mycon.commit()
    except psycopg2.DatabaseError as err:
        bb.logit(f'{err}')
    cur.close()
    mycon.close

def add_primary_provider_ids():
    num_provs = 200
    base_val = 1000000
    query_sql = "select m.id, m.member_id from member m;"
    mycon = pg_connection()
    cur = mycon.cursor()
    update_cur = mycon.cursor()
    try:
        cur.execute(query_sql)
        for item in cur:
            #print(f'item: {item}')
            pid = f'P-{random.randint(base_val, base_val + num_provs)}'
            sql = f'update member set primaryprovider_id = \'{pid}\' '
            sql += f"where id = {item[0]}"
            #print(sql)
            bb.logit(f'Update: {item[1]}')
            update_cur.execute(sql)
            mycon.commit()
    except psycopg2.DatabaseError as err:
        bb.logit(f'{err}')
    cur.close()
    mycon.close

def fix_member_guardian_ids():
    num_provs = 200
    base_val = 1000000
    #query_sql = "select id, m.member_guardian_id from member_guardian m;"
    query_sql = "select id, m.claim_claimline_id from claim_claimline m;"
    mycon = pg_connection()
    cur = mycon.cursor()
    update_cur = mycon.cursor()
    cnt = 1
    try:
        cur.execute(query_sql)
        for item in cur:
            #print(f'item: {item}')
            pid = f'ME-{base_val + cnt}'
            #sql = f'update member_guardian set member_guardian_id = \'{pid}\' '
            sql = f'update claim_claimline set claim_claimline_id = \'{pid}\' '
            sql += f"where id = {item[0]}"
            #print(sql)
            bb.logit(f'Update: {item[1]}')
            update_cur.execute(sql)
            mycon.commit()
            cnt += 1
    except psycopg2.DatabaseError as err:
        bb.logit(f'{err}')
    cur.close()
    mycon.close

#----------------------------------------------------------------------#
#   Queries
#----------------------------------------------------------------------#
def member_claims_api():
    sql = {}
    csql = "select c.*, m.firstname, m.last_name, m.dateofbirth, m.gender, clv.* "
    csql += "from vw_claim_claimline clv INNER JOIN claim c where c.patient_id = '__MEMBER_ID__'"
    csql += "INNER JOIN member m on m.member_id = c.patient_id "
    csql += "INNER JOIN"
    sql["member_claims"] = csql

def claimline_vw():
    vwsql = "create or replace view vw_claim_claimline AS \n"
    sql = "select cl.*, ap.firstname as ap_first, ap.lastname as ap_last, ap.gender as ap_gender, ap.dateofbirth as ap_birthdate, "
    sql += "op.firstname as op_first, op.lastname as op_last, op.gender as op_gender, op.dateofbirth as op_birthdate, "
    sql += "rp.firstname as rp_first, rp.lastname as rp_last, rp.gender as rp_gender, rp.dateofbirth as rp_birthdate, "
    sql += "opp.firstname as opp_first, opp.lastname as opp_last, opp.gender as opp_gender, opp.dateofbirth as opp_birthdate "
    sql += "from claim_claimline cl INNER JOIN provider ap on cl.attendingprovider_id = ap.provider_id "
    sql += "INNER JOIN provider op on cl.orderingprovider_id = op.provider_id "
    sql += "INNER JOIN provider rp on cl.referringprovider_id = rp.provider_id "
    sql += "INNER JOIN provider opp on cl.operatingprovider_id = opp.provider_id "

def member_api():
    # show a single member and recent claims
    # include the primary provider
    d_member = {}
    sql = "select m.*, p.nationalprovideridentifier, p.firstname, p.lastname, p.dateofbirth, p.gender from member m INNER JOIN provider p on p.provider_id = m.primaryprovider_id;" # INNER JOIN providers p on m.primaryProvider_id = p.provider_id"
    result = sql_query("healthcare", sql)
    k = 0
    for item in result:
        if k < 20:
            pprint.pprint(item)
        k += 1

def get_claims(conn = ''):
    conn = pg_connection()
    sql = "select * from claim"
    query_result = newsql_query(sql,conn)
    num_results = query_result["num_records"]
    print(f'Claims: {num_results}')
    ids = []
    for row in query_result["data"]:
        ids.append(row[1])
    result = get_claimlines(conn,ids)
    data = result["data"]
    inc = 0
    for k in data:
        print(f'#-------------------- {k} --------------------------')
        pprint.pprint(data[k])
        inc += 1
        if inc > 10:
            break
    conn.close()
      
def get_claimlines(conn,claim_ids):
    #payment_fields = sql_helper.column_names("claim_claimline_payment", conn)
    payment_fields = column_names("claim_claimline_payment", conn)
    pfields = ', p.'.join(payment_fields)
    ids_list = '\',\''.join(claim_ids)
    sql = f'select c.*, p.{pfields} from claim_claimline c '
    sql += 'INNER JOIN claim_claimline_payment p on c.claim_claimline_id = p.claim_claimline_id '
    sql += f'WHERE c.claim_id IN (\'{ids_list}\') '
    sql += "order by c.claim_id"
    #query_result = sql_helper.sql_query(sql, conn)
    query_result = newsql_query(sql,conn)
    num_results = query_result["num_records"]
    #claimline_fields = sql_helper.column_names("claim_claimline", conn)
    claimline_fields = column_names("claim_claimline", conn)
    num_cfields = len(claimline_fields)
    # Check if the records are found
    result = {"num_records" : num_results, "data" : []}
    data = {}
    if num_results > 0:
        last_id = "zzzzzz"
        firsttime = True
        for row in query_result["data"]:
            cur_id = row[1]
            doc = {}
            if cur_id != last_id:
                if not firsttime:
                    data[last_id] = docs
                    docs = []
                else:
                    docs = []
                    firsttime = False
                last_id = cur_id
            #print(row)
            for k in range(num_cfields):
                #print(claimline_fields[k])
                doc[claimline_fields[k]] = row[k]
            sub_doc = {}
            for k in range(len(payment_fields)):
                #print(payment_fields[k])
                sub_doc[payment_fields[k]] = row[k + num_cfields]
            doc["payment"] = sub_doc
            docs.append(doc)
            
    result["data"] = data
    return result

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

#----------------------------------------------------------------------#
#   Utility Routines
#----------------------------------------------------------------------#

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
    #print(sql)
    #print(vals)
    try:
        cur.executemany(sql, vals)
        conn.commit()
        bb.logit(f'{cur.rowcount} inserted')
    except psycopg2.DatabaseError as err:
        bb.logit(f'{table} - {err}')
    cur.close()
    if not nconn:
        conn.close()

def sql_query(database, sql, nconn = False):
    # insert_into table fields () values ();
    if nconn:
        conn = nconn
    else:
        conn = pg_connection('postgres', database)
    cur = conn.cursor()
    #print(sql)
    try:
        cur.execute(sql)
        bb.logit(f'{cur.rowcount} records')
    except psycopg2.DatabaseError as err:
        bb.logit(f'{sql} - {err}')
    result = cur.fetchall()
    cur.close()
    if not nconn:
        conn.close()
    return result

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

def newsql_query(sql, conn):
    # Simple query executor
    cur = conn.cursor()
    #print(sql)
    try:
        cur.execute(sql)
        row_count = cur.rowcount
        print(f'{row_count} records')
    except psycopg2.DatabaseError as err:
        print(f'{sql} - {err}')
    result = {"num_records" : row_count, "data" : cur.fetchall()}
    cur.close()
    return result

def column_names(table, conn):
    sql = f'SELECT column_name FROM information_schema.columns WHERE table_schema = \'public\' AND table_name   = \'{table}\''
    cur = conn.cursor()
    #print(sql)
    try:
        cur.execute(sql)
        row_count = cur.rowcount
        print(f'{row_count} columns')
    except psycopg2.DatabaseError as err:
        print(f'{sql} - {err}')
    rows = cur.fetchall()
    result = []
    for i in rows:
        result.append(i[0])
    cur.close()
    return result

def increment_version(old_ver):
    parts = old_ver.split(".")
    return(f'{parts[0]}.{int(parts[1]) + 1}')

def test_big_query():
    client = bigquery_connection()
    bb.logit("# --- Check Dataset --- #")
    datasets = list(client.list_datasets())
    for k in datasets:
        print(k)
    bb.logit("# --> Create Table")
    table_id = bigquery.Table.from_string("bradybyrd-poc.claims_warehouse.bb_stuff")
    schema = [
        bigquery.SchemaField("first_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dob", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("age", "INTEGER"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

def sql_action(conn, action, tables):
    if action == "none":
        return("no action")
    sql = ""
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
            print(sql)
            conn.commit()
            bb.logit(f"recovering...")
        else:
            print("OK")
            conn.commit()
    cursor.close()
    return("success")

def bigquery_connection(type = "bigquery", sdb = 'none'):
    # export GOOGLE_APPLICATION_CREDENTIALS="/Users/godwinekuma/tutorials/python-bigquery/service-account-file.json"
    client = bigquery.Client()
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
    elif ARGS["action"] == "load_data":
        load_bigquery_data()
    elif ARGS["action"] == "test_csv":
        result = build_batch_from_template({"template" : "model-tables/member.csv", "collection" : "notused", "batch_size" : 4})
        pprint.pprint(result)
    elif ARGS["action"] == "execute_ddl":
        execute_ddl()
    elif ARGS["action"] == "fix_providers":
        add_primary_provider_ids()
    elif ARGS["action"] == "fix_guardians":
        fix_member_guardian_ids()
    elif ARGS["action"] == "query_test":
        member_api()
    elif ARGS["action"] == "claim":
        get_claims()
    elif ARGS["action"] == "foreign_keys":
        create_foreign_keys()
    elif ARGS["action"] == "test":
        test_big_query()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()

'''
# ---------------------------------------------------- #

Create Database:
    python3 load_sql.py action=execute_ddl task=create
    python3 load_sql.py action=load_pg_data
'''
