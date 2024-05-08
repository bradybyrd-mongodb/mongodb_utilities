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
from google.cloud import bigquery

# import psycopg2
from faker import Faker
import itertools
from deepmerge import Merger
import uuid

fake = Faker()
providers = ["cigna", "aetna", "anthem", "bscbsma", "kaiser"]

#  BJB 5/15/23 Big Query
from google.cloud import bigquery


"""
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
"""
settings_file = "relations_settings.json"


def load_bigquery_data():
    # read settings and echo back
    bb.message_box("Loading Data to BigQuery", "title")
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
    if "size" in ARGS:
        if int(ARGS["size"]) < 1000:
            num_procs = 1
    jobs = []
    inc = 0
    #multiprocessing.set_start_method("fork", force=True)
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
    global settings, bb, ARGS, IDGEN, id_map, base_counter
    cur_process = multiprocessing.current_process()
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    base_counter = settings["base_counter"]
    IDGEN = Id_generator({"seed": base_counter})
    id_map = defaultdict(int)
    bb.message_box(f"({cur_process.name}) Loading Synth Data in SQL", "title")
    bqconn = bigquery_connection()
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
    # IDGEN = Id_generator({"seed" : base_counter, "size" : details["size"]})
    for domain in job_info:
        details = job_info[domain]
        batches = 1
        template_file = details["path"]
        count = details["size"]
        if "size" in ARGS:
            count = int(ARGS["size"])
        if "base" in ARGS:
            base_counter = int(ARGS["base"])
        prefix = details["id_prefix"]
        base_counter = settings["base_counter"] + count * ipos
        bb.message_box(domain, "title")
        table_info = ddl_from_template("none", bqconn, template_file, domain)
        if count > batch_size:
            batches = int(count / batch_size)
        IDGEN.set({"seed": base_counter, "size": count, "prefix": prefix})
        for k in range(batches):
            bb.logit(f"Loading batch: {k} - size: {batch_size}")
            result = build_sql_batch_from_template(
                table_info,
                {
                    "master": domain,
                    "connection": bqconn,
                    "template": template_file,
                    "batch": k,
                    "id_prefix": prefix,
                    "base_count": base_counter,
                    "size": count,
                },
            )

    end_time = datetime.datetime.now()
    time_diff = end_time - start_time
    execution_time = time_diff.total_seconds()
    bqconn.close()
    # file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")


# -----------------------------------------------------------#
#  CSV template sql translator
# -----------------------------------------------------------#
def build_sql_batch_from_template(tables, details={}):
    template_file = details["template"]
    batch_size = settings["batch_size"]
    if details["size"] < batch_size:
        batch_size = details["size"]
    base_counter = details["base_count"]
    num_procs = settings["process_count"]
    batch = details["batch"]
    master_table = details["master"]
    master_id = f"{master_table}_id".lower()
    cnt = 0
    tab_types = table_types(tables)
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
        counts = random.randint(1, 5) if len(cur_table.split("_")) > 1 else 1
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
                    # if type(cur_val) is bool:
                    #    cur_val = 'T' if cur_val == True else "F"
                    if type(cur_val) is datetime.datetime:
                        cur_val = cur_val.strftime("%Y-%m-%d %H:%M:%S")
                    fld_cnt += 1
                hsh[cur_field.lower()] = cur_val
            idpos += 1
            cnt += 1
            recs.append(hsh)
        # print("# ----------- Data ---------------------- #")
        # pprint.pprint(recs)
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
    gcp_project_id = settings["gcp_project_id"]
    gcp_dataset = settings["gcp_dataset"]
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
            if last_table != "zzzzz":
                # Add modified to each table:
                new_field = bigquery.SchemaField("modified_at", "DATETIME")
                tables[last_table]["schema"].append(new_field)
                tables[last_table]["fields"].append("modified_at")
                tables[last_table]["generator"].append("datetime.datetime.now()")
            # bb.logit("#--------------------------------------#")
            bb.logit(f"Building table: {table}")
            last_table = table
            fkey = ""
            flds = []
            schema = []

            #  Add a self_id field
            field_name = f"{table}_id".lower()
            new_field = bigquery.SchemaField(field_name, "STRING", mode="REQUIRED")
            flds.append(field_name)
            schema.append(new_field)
            if len(table.split("_")) > 1:
                #  Add a parent_id field
                field_name = f'{row["parent"]}_id'.lower()
                new_field = bigquery.SchemaField(field_name, "STRING", mode="REQUIRED")
                flds.append(field_name)
                schema.append(new_field)
            if field != field_name:
                new_field = bigquery.SchemaField(field, ftype)
                flds.append(field)
                schema.append(new_field)
            table_id = bigquery.Table.from_string(
                f"{gcp_project_id}.{gcp_dataset}.{table}"
            )

            tables[table] = {
                "table_id": table_id,
                "schema": schema,
                "database": database,
                "fields": flds,
                "generator": [row["generator"]],
                "parent": row["parent"],
            }

        else:
            # bb.logit(f'Adding table data: {table}, {field}')
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
    return tables

def master_from_file(file_name):
    return file_name.split("/")[-1].split(".")[0]

def bigquery_changer():
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    table = "bbwarehouse.Provider"
    batch_no = 1000
    bb.message_box(f"Simulate changes in BigQuery", "title")
    bqconn = bigquery_connection()
    settings = bb.read_json(settings_file)
    tot_processed = 0
    sql = f"SELECT * FROM {table} WHERE RAND() < 0.1 LIMIT 10"
    for iter in range(1000):
        cur_docs = []
        bb.logit(f"Checking for changes ({cur_date}): ")
        cur_date = datetime.datetime.now()
        fmt_time = cur_date.strftime('%Y-%m-%d %H:%M:%S')
        ids = ""
        ans = bqconn.query(sql)
        for row in ans.result():
            cur_id = row.provider_id
            ids += f"'{cur_id}', "
            tot_processed += 1
        ids = ids.rstrip(',')
        sql_update = f"UPDATE {table} SET modified_at = '{fmt_time}' WHERE provider_id IN ({ids})"
        batch_no += 1
        bb.logit("Waiting to make changes")
        time.sleep(10)
        if iter > 3:
            break
    bb.logit("All done")

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
        # ddl = tables_obj[tab]["ddl"]
        # l = len(ddl)
        # ddl = ddl[:l-1] + ")"
        # tables_obj[tab]["ddl"] = ddl
        fmts = value_codes(tables_obj[tab]["fields"])
        tables_obj[tab][
            "insert"
        ] = f'insert into {tab} ({",".join(tables_obj[tab]["fields"])}) values ({fmts});'


def gcp_type(mtype):
    type_x = {
        "string": "STRING",
        "boolean": "BOOL",
        "date": "DATETIME",
        "integer": "INT64",
        "double": "FLOAT64",
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
        # support for parent.child.child.field, parent.children().field
        for row in propreader:
            result = {
                "name": "",
                "table": "",
                "parent": "",
                "type": gcp_type(row[1]),
                "generator": generator_values(row[3]),
            }
            path = row[0].split(".")
            depth = len(path)
            for k in range(depth):
                path[k] = path[k].strip("()").strip()
            result["name"] = path[-1]
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
    mycon.close

def get_claimlines(conn, claim_ids):
    # payment_fields = sql_helper.column_names("claim_claimline_payment", conn)
    payment_fields = column_names("claim_claimline_payment", conn)
    pfields = ", p.".join(payment_fields)
    ids_list = "','".join(claim_ids)
    sql = f"select c.*, p.{pfields} from claim_claimline c "
    sql += "INNER JOIN claim_claimline_payment p on c.claim_claimline_id = p.claim_claimline_id "
    sql += f"WHERE c.claim_id IN ('{ids_list}') "
    sql += "order by c.claim_id"
    # query_result = sql_helper.sql_query(sql, conn)
    query_result = newsql_query(sql, conn)
    num_results = query_result["num_records"]
    # claimline_fields = sql_helper.column_names("claim_claimline", conn)
    claimline_fields = column_names("claim_claimline", conn)
    num_cfields = len(claimline_fields)
    # Check if the records are found
    result = {"num_records": num_results, "data": []}
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
            # print(row)
            for k in range(num_cfields):
                # print(claimline_fields[k])
                doc[claimline_fields[k]] = row[k]
            sub_doc = {}
            for k in range(len(payment_fields)):
                # print(payment_fields[k])
                sub_doc[payment_fields[k]] = row[k + num_cfields]
            doc["payment"] = sub_doc
            docs.append(doc)

    result["data"] = data
    return result


# ----------------------------------------------------------------------#
#   CSV Loader Routines
# ----------------------------------------------------------------------#
stripProp = lambda str: re.sub(r"\s+", "", (str[0].upper() + str[1:].strip("()")))


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
    fields = list(recs[0])
    table_id = tables[table]["table_id"]
    vals = []
    bb.logit(f"Loading into BigQuery: {len(recs)}")
    try:
        errors = nconn.insert_rows_json(table_id, recs)
        if errors == []:
            bb.logit(f"{len(recs)} inserted")
        else:
            bb.logit("Encountered errors while inserting rows: {}".format(errors))
    except Exception as err:
        bb.logit(f"{table} - {err}")


def sql_query(database, sql, nconn=False):
    # insert_into table fields () values ();

    try:
        nconn.execute(sql)
        bb.logit(f"{cur.rowcount} records")
    except Exception as err:
        bb.logit(f"{sql} - {err}")
    result = nconn.fetchall()
    if not nconn:
        nconn.close()
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

def increment_version(old_ver):
    parts = old_ver.split(".")
    return f"{parts[0]}.{int(parts[1]) + 1}"


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
    if action == "none" or action == "info":
        return "no action"
    sql = ""
    # cursor = conn.cursor()
    for table_name in tables:
        if action == "create":
            schema = tables[table_name]["schema"]
            table_id = tables[table_name]["table_id"]
            table = bigquery.Table(table_id, schema=schema)
            table = conn.create_table(table)
            print(
                "Created BigQTable {}.{}.{}".format(
                    table.project, table.dataset_id, table.table_id
                )
            )
        elif action == "drop":
            conn.delete_table(tables[table_name]["table_id"], not_found_ok=True)
            print(f"Deleted: {table_name}")
        elif action == "delete":
            sql = f"delete from {table_name};"

        """
        try:
            bb.logit(f"Action: {action} {table_name}")
            print(sql)
            cursor.execute(sql)
        except Exception as err:
            bb.logit(pprint.pformat(err))
            print(sql)
            conn.commit()
            bb.logit(f"recovering...")
        else:
            print("OK")
            conn.commit()
    cursor.close()
    """
    return "success"

def bigquery_connection(type="bigquery", sdb="none"):
    # export GOOGLE_APPLICATION_CREDENTIALS="/Users/brady.byrd/Documents/mongodb/dev/servers/gcp-pubsub-user/bradybyrd-poc-ac0790ea4120.json"
    client = bigquery.Client()
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
    elif ARGS["action"] == "load_data":
        # python3 load_sql_gcp.py action=load_data template=model-tables/provider.csv size=15 base=2002000
        # python3 load_sql_gcp.py action=load_data - loads from "data" key in settings
        load_bigquery_data()
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
    elif ARGS["action"] == "changer":
        bigquery_changer()
    else:
        print(f'{ARGS["action"]} not found')
    # conn.close()

"""
# ---------------------------------------------------- #

Create Database:
    python3 load_sql.py action=execute_ddl task=create
    python3 load_sql.py action=load_pg_data



BigQuery:
-- SELECT provider_id, modified_at FROM `bradybyrd-poc.bbwarehouse.Provider` where provider_id = 'P-2002001' LIMIT 1000
--delete from bbwarehouse.Provider where modified_at > '2024-04-01'
select * from bbwarehouse.Provider where provider_id > 'P-2001998'

"""
