import sys
import os
import csv

# import vcf
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
base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(base_dir))

from bbutil import Util
from id_generator import Id_generator
from pymongo import MongoClient
import psycopg2
from faker import Faker
import itertools
from deepmerge import Merger
import uuid
import bldg_mix as mix

fake = Faker()

settings_file = "site_settings.json"

def load_postgres_data():
    # read settings and echo back
    bb.message_box("Loading Data", "title")
    bb.logit(f"# Settings from: {settings_file}")
    passed_args = {"ddl_action": "info"}
    if "template" in ARGS:
        template = ARGS["template"]
        passed_args["template"] = template
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
        p = multiprocessing.Process(target=worker_load, args=(item, passed_args))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit("Main process is %s %s" % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def load_from_csv():
    # Provider.specialties().status,String,optional,"fake.random_element(('Active', 'Inactive'))","Active, Inactive",,,,Approved,,Provider.CredentialedSpecialties().Status,Embed-PegaHC-Stringlist,7.21
    boo = "boo"

def worker_load(ipos, args):
    #  Reads EMR sample file and finds values
    cur_process = multiprocessing.current_process()
    bb.message_box(f"({cur_process.name}) Loading Synth Data in SQL", "title")
    pgconn = None #pg_connection()
    conn = None #client_connection()
    settings = bb.read_json(settings_file)
    batches = settings["batches"]
    batch_size = settings["batch_size"]
    if "template" in args:
        template = args["template"]
        master_table = master_from_file(template)
        job_info = {
            master_table: {
                "path": template,
                "size": settings["batches"] * settings["batch_size"],
                "id_prefix": f"{master_table[0].upper()}-"
            }
            
        }
    else:
        job_info = settings["data"]
    start_time = datetime.datetime.now()
    seed_data = mix.init_seed_data(conn, IDGEN, settings)
    CurInfo = mix.CurItem({"version" : settings["version"], "addr_info" : seed_data["addr_info"], "sites" : seed_data["sites"]})
    pprint.pprint(CurInfo.get_item("none", "A-107"))
    bb.logit(f'Portfolio: {CurInfo.get_item("portfolio_name")}, len: {len(seed_data["sites"])}')
    
    # IDGEN = Id_generator({"seed" : base_counter, "size" : details["size"]})
    for domain in job_info:
        details = job_info[domain]
        template_file = details["path"]
        multiplier = details["multiplier"]
        count = batches * batch_size * multiplier
        
        prefix = details["id_prefix"]
        base_counter = settings["base_counter"] + count * ipos
        bb.message_box(domain, "title")
        table_info = ddl_from_template("none", pgconn, template_file, domain)
        IDGEN.set({"seed": base_counter, "size": count, "prefix": prefix})
        for k in range(batches):
            bb.logit(f"Loading batch: {k} - size: {batch_size}")
            result = build_sql_batch_from_template(
                table_info,
                {
                    "master": domain,
                    "connection": pgconn,
                    "template": template_file,
                    "batch": k,
                    "id_prefix": prefix,
                    "base_count": base_counter,
                    "size": count,
                    "cur_info" : CurInfo
                },
            )

    end_time = datetime.datetime.now()
    time_diff = end_time - start_time
    execution_time = time_diff.total_seconds()
    pgconn.close()
    # file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

# -----------------------------------------------------------#
#  CSV template sql translator
# -----------------------------------------------------------#
def build_sql_batch_from_template(tables, details={}):
    template_file = details["template"]
    batch_size = settings["batch_size"]
    base_counter = details["base_count"]
    num_procs = settings["process_count"]
    batch = details["batch"]
    target = "sql"
    master_table = details["master"]
    master_id = f"{master_table}_id".lower()
    cnt = 0
    tab_types = table_types(tables)
    cur_info = None
    if "size" in details and details["size"] < batch_size:
        batch_size = details["size"]
    if "cur_info" in details:
        cur_info = details["cur_info"]
    bb.logit(f"Master: {master_table} - building: {batch_size}")
    master_ids = []
    rec_counts = {}
    g_id = ""
    database = "none"
    data = {}
    for item in tables:
        attrs = tables[item]
        cur_table = item
        parent = attrs["parent"]
        sub_size = attrs["sub_size"]
        table_type = tab_types[cur_table]
        recs = []
        bb.logit(f"Table: {cur_table} building data")
        database = attrs["database"]
        if table_type == "submaster":
            prefx = cur_table[0:2].upper() + "-"
            count = details["size"] * num_procs
            IDGEN.set({"seed": base_counter, "size": count, "prefix": prefx})
        elif table_type == "none" and len(parent.split("_")) > 1:
            id_prefix = parent[0:2].upper() + "-"
        else:
            id_prefix = details["id_prefix"]
        #counts = random.randint(1, 5) if len(cur_table.split("_")) > 1 else 1
        counts = sub_size if len(cur_table.split("_")) > 1 else 1
        bb.logit(f"Table: {cur_table} building data, factor: {counts}")
        sql = attrs["insert"]
        idpos = 0
        rec_counts[cur_table] = batch_size * counts * num_procs
        for inc in range(
            0, batch_size * counts
        ):  # iterate through the bulk insert count
            fld_cnt = 0
            hsh = {}
            if idpos > batch_size - 1:
                idpos = 0
            for cur_field in attrs["fields"]:
                # bb.logit(f'Field: {cur_field} - gen {attrs["generator"][fld_cnt]}')
                if table_type == "master" and cur_field.lower() == master_id:
                    # master e.g. claim_id
                    g_id = eval(attrs["generator"][fld_cnt])
                    cur_val = g_id
                    master_ids.append(g_id)
                    # bb.logit(f'[{cnt}] - GlobalID = {g_id}')
                    # is_master = False
                    fld_cnt += 1
                elif cur_field.lower() == master_id:
                    # bb.logit(f'IDPOS: {idpos}')
                    cur_val = master_ids[idpos]
                elif cur_field.lower().replace("_id", "") == attrs["parent"].lower():
                    # child of e.g. claim_claimline.claim_id
                    cur_val = IDGEN.random_value(id_prefix)
                    # bb.logit(f'IDsub[{cur_val}] {cur_table} - {attrs["parent"]}\n{IDGEN.value_history}')
                elif cur_field.lower() == f"{cur_table.lower()}_id":
                    # Internal id for table
                    prefx = cur_table[0:2].upper() + "-"
                    if table_type == "submaster":
                        cur_val = IDGEN.get(prefx)
                    else:
                        cur_val = f"{prefx}{random.randint(1000,1000000)}"
                else:
                    cur_val = eval(attrs["generator"][fld_cnt])
                    if type(cur_val) is bool:
                        cur_val = "T" if cur_val == True else "F"
                    fld_cnt += 1
                hsh[cur_field.lower()] = cur_val
            idpos += 1
            cnt += 1
            recs.append(hsh)
        record_loader(tables, cur_table, recs, details["connection"])
        bb.logit(f"{batch_size} {cur_table} batch complete (tot = {cnt})")
    bb.logit(f"{cnt} records for {database} complete")
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
    # pprint.pprint(fields)
    tables = {}
    last_table = "zzzzz"
    ddl = ""
    for row in fields:
        table, field, ftype = clean_field_data(row)
        if table not in tables:
            # bb.logit("#--------------------------------------#")
            bb.logit(f"Building table: {table}")
            last_table = table
            fkey = ""
            flds = [field]
            if len(table.split("_")) > 1:
                #  Add a parent_id field
                new_field = stripProp(f'{row["parent"]}_id')
                fkey = f"  {new_field} varchar(20) NOT NULL,"
                flds.append(new_field)
                #  Add a self_id field
                new_field = stripProp(f"{table}_id")
                fkey += f"  {new_field} varchar(20) NOT NULL,"
                flds.append(new_field)
            ddl = (
                f"CREATE TABLE {table} ("
                "  id SERIAL PRIMARY KEY,"
                f"{fkey}"
                f"  {field} {ftype},"
            )
            tables[table] = {
                "ddl": ddl,
                "database": database,
                "fields": flds,
                "generator": [row["generator"]],
                "parent": row["parent"],
                "sub_size" : row["sub_size"]
            }

        else:
            # bb.logit(f'Adding table data: {table}, {field}')
            if field not in tables[table]["fields"]:
                tables[table]["ddl"] = tables[table]["ddl"] + f"  {field} {ftype},"
                tables[table]["fields"].append(field)
                tables[table]["generator"].append(row["generator"])
        first = False
    clean_ddl(tables)
    bb.logit("Table DDL:")
    pprint.pprint(tables)
    sql_action(pgconn, action, tables)
    return tables

def master_from_file(file_name):
    return file_name.split("/")[-1].split(".")[0]

def clean_field_data(data):
    tab = data["table"]
    if data["name"].lower() == "id":
        data["name"] = f"{tab}_id"
    if (
        len(tab.split("_")) == 2 and tab.split("_")[0] == tab.split("_")[1]
    ):  # "catch doubled eg member_member"
        tab = tab.split("_")[0]
    return tab, data["name"], data["type"]

def clean_ddl(tables_obj):
    for tab in tables_obj:
        ddl = tables_obj[tab]["ddl"]
        l = len(ddl)
        ddl = ddl[: l - 1] + ")"
        tables_obj[tab]["ddl"] = ddl
        fmts = value_codes(tables_obj[tab]["fields"])
        tables_obj[tab][
            "insert"
        ] = f'insert into {tab} ({",".join(tables_obj[tab]["fields"])}) values ({fmts});'

def pg_type(mtype):
    type_x = {
        "string": "varchar(100)",
        "boolean": "varchar(2)",
        "date": "timestamp",
        "integer": "integer",
        "text": "text",
        "double": "real",
    }
    ftype = type_x[mtype.lower().strip()]
    return ftype

def generator_values(gen):
    # substitute params for generic generator values
    return gen

def fields_from_template(template):
    """
    {'name': 'EffectiveDate', 'table': 'member_address', 'type': 'date', 'generator' : "fake.date()", 'parent' : 'member'},
    """
    ddl = []
    with open(template) as csvfile:
        propreader = csv.reader(itertools.islice(csvfile, 1, None))
        master = ""
        sub_size = 1
        # support for parent.child.child.field, parent.children().field
        for row in propreader:
            path = row[0].split(".")
            if "CONTROL" in row[0]:
                continue
            result = {
                "name": "",
                "table": "",
                "parent": "",
                "type": pg_type(row[1]),
                "generator": generator_values(row[3]),
                "sub_size" : sub_size
            }
            depth = len(path)
            for k in range(depth):
                if path[k].endswith(')'):
                    res = re.findall(r'\(.*\)',path[k])[0]
                    if res != "()":
                        sub_size = int(res.replace("(","").replace(")",""))
                    else:
                        sub_size = random.randint(1,5)
                    path[k] = path[k].replace(res,"")
            result["name"] = path[-1]
            result["sub_size"] = sub_size
            master = f"{path[0]}"
            if depth == 1:
                bb = ""  # should never see this
            elif depth == 2:
                result["table"] = master
            elif depth == 3:
                result["table"] = f"{path[0]}_{path[1]}"
                result["parent"] = master
            elif depth == 4:
                result["table"] = f"{path[0]}_{path[1]}_{path[2]}"
                result["parent"] = f"{path[0]}_{path[1]}"
            elif depth == 5:
                result["table"] = f"{path[0]}_{path[1]}_{path[2]}_{path[3]}"
                result["parent"] = f"{path[0]}_{path[1]}_{path[2]}"
            ddl.append(result)
    return ddl

def execute_ddl(ddl_action="info"):
    ddl_action = "info"
    mycon = None
    if "template" in ARGS:
        template = ARGS["template"]
    elif "data" in settings:
        goodtogo = True
    else:
        print("Send template=<pathToTemplate>")
        sys.exit(1)
    if "task" in ARGS:
        ddl_action = ARGS["task"]
    if ddl_action != "info":
        mycon = pg_connection()
    if "template" in ARGS:
        master_table = master_from_file(template)
        job_info = {
            master_table: {
                "path": template,
                "size": settings["batches"] * settings["batch_size"],
                "id_prefix": f"{master_table[0].upper()}-",
            }
        }
    else:
        job_info = settings["data"]
    start_time = datetime.datetime.now()
    for domain in job_info:
        details = job_info[domain]
        bb.message_box(domain, "title")
        bb.logit(details["path"])
        template_file = details["path"]
        table_info = ddl_from_template(ddl_action, mycon, template_file, domain)
    if mycon != None:
        mycon.close

def create_foreign_keys():
    #  Reads settings file and finds values
    cur_process = multiprocessing.current_process()
    bb.message_box(f"({cur_process.name}) Creating Foreign Keys in SQL", "title")
    start_time = datetime.datetime.now()
    pgconn = pg_connection()
    settings = bb.read_json(settings_file)
    cur = pgconn.cursor()
    cur2 = pgconn.cursor()
    sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    cur.execute(sql)
    for item in cur:
        bb.logit(f"item: {item}")
        try:
            fkey_sql = foreign_key_sql(item[0])
            if fkey_sql != "none":
                print(fkey_sql)
                cur2.execute(fkey_sql)
                pgconn.commit()
        except psycopg2.DatabaseError as err:
            bb.logit(f"{err}", "ERROR")
            pgconn.commit()
            # cur2.close()
            # cur2 = pgconn.cursor()
    cur.close()
    cur2.close()
    end_time = datetime.datetime.now()
    time_diff = end_time - start_time
    execution_time = time_diff.total_seconds()
    pgconn.close()
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def foreign_key_sql(table):
    parts = table.split("_")
    part_size = len(parts)
    child = parts[-1]
    if part_size == 1:
        return "none"
    elif part_size == 2:
        parent = parts[0]
    elif part_size == 3:
        parent = f"{parts[0]}_{parts[1]}"
    fkey = f"{parent}_id"
    sql = (
        f"ALTER TABLE IF EXISTS public.{table}\n"
        f"ADD CONSTRAINT fky_{fkey} FOREIGN KEY ({fkey})\n"
        f"REFERENCES public.{parent} ({fkey}) MATCH SIMPLE\n"
        f"ON UPDATE NO ACTION\n"
        f"ON DELETE NO ACTION\n"
        f" NOT VALID"
    )
    return sql

# ----------------------------------------------------------------------#
#   CSV Loader Routines
# ----------------------------------------------------------------------#
#stripProp = lambda str: re.sub(r"\s+", "", (str[0].upper() + str[1:].strip("()")))
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
        return {stripped: eval(generator)}
    elif path[0].endswith("()"):
        # Lists are slightly more complex. We generate a list of the length specified in the
        # counts map. Note that what we pass recursively is _the exact same path_, but we strip
        # off the ()s, which will cause us to hit the `else` block below on recursion.
        return {
            stripped: [
                procpath([path[0].strip("()")] + path[1:], counts, generator)[stripped]
                for X in range(0, counts[stripped])
            ]
        }
    else:
        # Return a nested page, of the specified type, populated recursively.
        return {stripped: procpath(path[1:], counts, generator)}

def zipmerge(the_merger, path, base, nxt):
    """Strategy for deepmerge that will zip merge two lists. Assumes lists of equal length."""
    return [the_merger.merge(base[i], nxt[i]) for i in range(0, len(base))]

def ID(key):
    id_map[key] += 1
    return key + str(id_map[key] + base_counter)

def local_geo():
    coords = fake.local_latlng("US", True)
    return coords

# ----------------------------------------------------------------------#
#   Utility Routines
# ----------------------------------------------------------------------#

def record_loader(tables, table, recs, nconn=False):
    # insert_into table fields () values ();
    if nconn:
        conn = nconn
    else:
        conn = pg_connection("postgres", tables[table]["database"])
    cur = conn.cursor()
    fields = list(recs[0])
    sql = tables[table]["insert"]
    vals = []
    for record in recs:
        stg = list()
        for k in record:
            stg.append(record[k])
        vals.append(tuple(stg))
    # print(sql)
    # print(vals)
    try:
        cur.executemany(sql, vals)
        conn.commit()
        bb.logit(f"{cur.rowcount} inserted")
    except psycopg2.DatabaseError as err:
        bb.logit(f"{table} - {err}")
    cur.close()
    if not nconn:
        conn.close()

def sql_query(database, sql, nconn=False):
    # insert_into table fields () values ();
    if nconn:
        conn = nconn
    else:
        conn = pg_connection("postgres", database)
    cur = conn.cursor()
    # print(sql)
    try:
        cur.execute(sql)
        bb.logit(f"{cur.rowcount} records")
    except psycopg2.DatabaseError as err:
        bb.logit(f"{sql} - {err}")
    result = cur.fetchall()
    cur.close()
    if not nconn:
        conn.close()
    return result

def value_codes(flds, special={}):
    result = ""

    for i in range(len(flds)):
        fmt = "%s"
        if i in special:
            fmt = special[i]
        if i == 0:
            result = fmt
        else:
            result += f", {fmt}"
    return result

def newsql_query(sql, conn):
    # Simple query executor
    cur = conn.cursor()
    # print(sql)
    try:
        cur.execute(sql)
        row_count = cur.rowcount
        print(f"{row_count} records")
    except psycopg2.DatabaseError as err:
        print(f"{sql} - {err}")
    result = {"num_records": row_count, "data": cur.fetchall()}
    cur.close()
    return result

def column_names(table, conn):
    sql = f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = '{table}'"
    cur = conn.cursor()
    # print(sql)
    try:
        cur.execute(sql)
        row_count = cur.rowcount
        print(f"{row_count} columns")
    except psycopg2.DatabaseError as err:
        print(f"{sql} - {err}")
    rows = cur.fetchall()
    result = []
    for i in rows:
        result.append(i[0])
    cur.close()
    return result

def increment_version(old_ver):
    parts = old_ver.split(".")
    return f"{parts[0]}.{int(parts[1]) + 1}"

def sql_action(conn, action, tables):
    if action == "none" or action == "info":
        return "no action"
    sql = ""
    cursor = conn.cursor()
    for table_name in tables:
        if action == "create":
            sql = tables[table_name]["ddl"]
        elif action == "drop":
            sql = f"DROP TABLE {table_name};"
        elif action == "delete":
            sql = f"delete from {table_name};"
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
    return "success"

def create_indexes():
    boo = "boo"
    indexes = [
        ["claim","claim_id"],
        ["claim","patient_id"],
        ["clamline","claim_id"],
        ["claim_note","claim_id"],
        ["claim_payment","claim_id"]
    ]
    return boo

def pg_connection(type="postgres", sdb="none"):
    # cur = mydb.cursor()
    # cur.execute("select * from Customer")
    # result = cursor.fetchall()
    shost = settings[type]["host"]
    susername = settings[type]["username"]
    spwd = settings[type]["password"]
    if "secret" in spwd:
        spwd = os.environ.get("_PGPWD_")
    if sdb == "none":
        sdb = settings[type]["database"]
    conn = psycopg2.connect(host=shost, database=sdb, user=susername, password=spwd)
    return conn

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

# ------------------------------------------------------------------#
#     MAIN
# ------------------------------------------------------------------#
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    base_counter = settings["base_counter"]
    IDGEN = Id_generator({"seed": base_counter})
    id_map = defaultdict(int)
    MASTER_CUSTOMERS = []
    if "wait" in ARGS:
        interval = int(ARGS["wait"])
        if interval > 10:
            bb.logit(f"Delay start, waiting: {interval} seconds")
            time.sleep(interval)
    # conn = client_connection()
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "load_pg_data":
        load_postgres_data()
    elif ARGS["action"] == "test_csv":
        result = build_batch_from_template(
            {
                "template": "model-tables/member.csv",
                "collection": "notused",
                "batch_size": 4,
            }
        )
        pprint.pprint(result)
    elif ARGS["action"] == "execute_ddl":
        execute_ddl()
    elif ARGS["action"] == "show_ddl":
        action = "none"
        pgconn = None
        template = ARGS["template"]
        domain = "bugsy"
        ddl_from_template(action, pgconn, template, domain)
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
    else:
        print(f'{ARGS["action"]} not found')
    # conn.close()

"""
# ---------------------------------------------------- #

Create Database:
    python3 load_sql.py action=execute_ddl task=create
    python3 load_sql.py action=load_pg_data
"""
