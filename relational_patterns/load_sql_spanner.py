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
import string
import getopt
import bson
from bson.objectid import ObjectId
from bson.json_util import dumps
from bbutil import Util
from id_generator import Id_generator
from pymongo import MongoClient
from google.cloud import spanner, spanner_admin_database_v1
from google.cloud.spanner_admin_database_v1.types.common import DatabaseDialect
from google.cloud.spanner_v1 import param_types
from google.cloud.spanner_v1.data_types import JsonObject
from google.cloud.spanner_admin_database_v1.types import spanner_database_admin

# import psycopg2
from faker import Faker
import itertools
from deepmerge import Merger
import uuid

global settings_file, settings
OPERATION_TIMEOUT_SECONDS = 240
fake = Faker()
letters = string.ascii_uppercase
providers = ["cigna", "aetna", "anthem", "bscbsma", "kaiser"]
#settings_file = ""

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

# Startup Env:
    
"""

def load_spanner_data():
    # read settings and echo back
    bb.message_box("Loading Data to Spanner", "title")
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
    pid = cur_process.name.replace("rocess","")
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    id_map = defaultdict(int)
    bb.message_box(f"({pid}) Loading Synth Data in SQL", "title")
    db_client = spanner_connection()
    settings = bb.read_json(settings_file)
    batches = settings["batches"]
    batch_size = settings["batch_size"]
    base_counter = settings["base_counter"] + batches * batch_size * ipos * 5
    base_counter_sub = settings["base_counter"] + batches * batch_size * ipos * 5
    if "template" in args:
        template = args["template"]
        master_table = master_from_file(template)
        job_info = {
            master_table: {
                "path": template,
                "size": batches * batch_size,
                "id_prefix": f"{master_table[0].upper()}-"
            }
        }
    else:
        job_info = settings["data"]
    IDGEN = Id_generator({"seed" : base_counter, "size" : batches * batch_size})
    start_time = datetime.datetime.now()
    for domain in job_info:
        details = job_info[domain]
        prefix = details["id_prefix"]
        IDGEN.set({"seed": base_counter, "size": details["size"], "prefix": prefix})
        template_file = details["path"]
        count = details["size"]
        batches = int(details["size"] / batch_size)
        bb.message_box(domain, "title")
        table_info = ddl_from_template("none", db_client, template_file, domain)        
        for k in range(batches):
            bb.logit(f"Loading batch: {k} - size: {batch_size}")
            result = build_sql_batch_from_template(
                table_info,
                {
                    "master": domain,
                    "connection": db_client,
                    "template": template_file,
                    "batch": k,
                    "id_prefix": prefix,
                    "base_count": base_counter,
                    "base_count_sub": base_counter_sub,
                    "size": count,
                    "pid" : pid,
                    "id_gen" : IDGEN
                },
            )

    end_time = datetime.datetime.now()
    time_diff = end_time - start_time
    execution_time = time_diff.total_seconds()
    db_client.close()
    # file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")


# -----------------------------------------------------------#
#  CSV template sql translator
# -----------------------------------------------------------#
def build_sql_batch_from_template(tables, details={}):
    global settings
    IDGEN = details["id_gen"]
    template_file = details["template"]
    pid = details["pid"]
    batch_size = settings["batch_size"]
    base_counter = details["base_count"]
    ID_MIN = base_counter
    ID_MAX = base_counter + details["size"]
    num_procs = settings["process_count"]
    master_table = details["master"].lower()
    master_id_field = f"{master_table}_id".lower()
    cnt = 0
    tab_types = table_types(tables)
    #pprint.pprint(tab_types)
    bb.logit(f"Master: {master_table} - building: {batch_size}")
    master_ids = []
    rec_counts = {}
    cur_id = ""
    database = "none"
    for item in tables:
        attrs = tables[item]
        cur_table = item
        parent = attrs["parent"]
        table_type = tab_types[cur_table]
        generator = ""
        batch = details["batch"]
        pair = cur_table.split("_")
        prefix = id_prefix(cur_table)
        id_field = f"{cur_table}_id".lower()
        parent_id_field = f"{parent}_id".lower() if parent != "" else ""
        recs = []
        counts = random.randint(1, 5) if len(cur_table.split("_")) > 1 else 1
        bb.logit(f"Table: {cur_table} building data, factor: {counts}")
        database = attrs["database"]
        if table_type == "master":
            prefix = details["id_prefix"]
        elif table_type == "submaster":
            #bb.logit("set submaster: ")
            count = details["size"] * num_procs * counts * batch
            #IDGEN.set({"seed": base_counter, "size": count, "prefix": prefix})
        idpos = 0
        rec_counts[cur_table] = batch_size * counts * num_procs
        for inc in range(0, batch_size * counts):  # iterate through the bulk insert count
            use_sequence = False
            fld_cnt = 0
            hsh = {}
            if idpos > batch_size - 1:
                idpos = 0
            # Clarify the ID
            if table_type == "master":
                generator = attrs["generator"][fld_cnt]
                cur_id = eval(generator)
                master_ids.append(cur_id)
                fld_cnt += 1
            elif len(pair) == 2 or table_type == "submaster":
                cur_id = IDGEN.get(prefix)
                generator = f"IDGEN({prefix})"
                #fld_cnt += 1
            else:
                cur_id = "seq" #IDGEN.random_value(prefix)
                use_sequence = True
                generator = f"sequence"
            # Loop through each field and gen a value
            for cur_field in attrs["fields"]:
                if cur_field.lower().endswith("_id") and master_table in cur_field:
                    if table_type != "master" and cur_field.lower() == master_id_field:
                        # related to master - bb.logit(f'IDPOS: {idpos}')
                        cur_val = master_ids[idpos]
                        generator = f"random-master"
                    elif cur_field == parent_id_field:
                        # parent_id = child of e.g. claim_claimline.claim_id
                        cur_val = IDGEN.random_value(id_prefix(parent))
                        generator = f"random-{parent}"
                        #fld_cnt += 1
                    else:
                        cur_val = cur_id
                else:
                    #  Your basic field value
                    generator = attrs["generator"][fld_cnt]
                    cur_val = eval(generator)
                    if type(cur_val) is bool:
                        #cur_val = 'true' if cur_val == True else "false"
                        letitbe = "yes"
                    elif cur_field == "modified_at":
                        letitbe = "yes"
                    elif type(cur_val) is datetime.datetime:
                        cur_val = cur_val.strftime("%Y-%m-%d")
                    fld_cnt += 1
                if use_sequence and cur_field == id_field:
                    letitbe = "yes" #Ignore auto-inc field
                else:
                    hsh[cur_field.lower()] = cur_val
                #bb.logit(f'{cur_table}: {cur_field} | {cur_val} | gen {generator}')
                
            idpos += 1
            cnt += 1
            recs.append(hsh)
        # print("# ----------- Data ---------------------- #")
        # pprint.pprint(recs)
        record_loader(tables, cur_table, recs, details["connection"])
        bb.logit(f"{batch_size} {cur_table} batch complete (tot = {cnt})")
    bb.logit(f"{cnt} records for {database} complete")
    return cnt

def id_prefix(table):
    pair = table.split("_")
    prefix = pair[0][0]
    if len(pair) > 2:
        prefix = f'{pair[0][0]}{pair[1][0]}{pair[2][0]}-'
    elif len(pair) == 2:
        prefix = f'{pair[0][0]}{pair[1][0]}-'
    return prefix.upper()

def table_types(table_info):
    res = {}
    subs = []
    for tab in table_info:
        cur_parent = table_info[tab]["parent"]
        if cur_parent != "" and cur_parent not in subs:
            subs.append(cur_parent)

    for tab in table_info:
        res[tab] = "none"
        attrs = table_info[tab]
        if attrs["parent"] == "":
            res[tab] = "master"
        elif tab in subs:
            res[tab] = "submaster"
    return res


def ddl_from_template(action, db_client, template, domain):
    database = settings["spanner"]["database_id"]
    bb.message_box("Generating DDL")
    # Read the csv file and digest
    fields = fields_from_template(template)
    # pprint.pprint(fields)
    tables = {}
    last_table = "zzzzz"
    for row in fields:
        table, field, ftype = clean_field_data(row)
        if table not in tables:
            if last_table != "zzzzz":
                ddl_post_actions(tables, last_table)
            # bb.logit("#--------------------------------------#")
            bb.logit(f"Building table: {table}")
            last_table = table
            flds = []
            schema = []
            sequence_id = ""
            t_depth = len(table.split("_"))
            schema.append(f'CREATE TABLE {table} (')
            #  Add a self_id field
            field_name = f"{table}_id".lower()
            new_field = f'{field_name}   {gcp_type('string')} NOT NULL'
            if t_depth > 2:
                # use auto-seq for deep tables
                sequence_id = f"CREATE SEQUENCE {field_name}_seq bit_reversed_positive;"
                new_field = f"{field_name}   INT DEFAULT nextval('{field_name}_seq')"
            schema.append(new_field)
            flds.append(field_name)
            if t_depth > 1:
                #  Add a parent_id field
                field_name = f'{row["parent"]}_id'.lower()
                new_field = f'{field_name}   {gcp_type('string')} NOT NULL'
                flds.append(field_name)
                schema.append(new_field)
            if field != field_name:
                new_field = f'{field}   {ftype}'
                flds.append(field)
                schema.append(new_field)

            tables[table] = {
                "extra_ddl": sequence_id,
                "schema": schema,
                "database": database,
                "fields": flds,
                "generator": [row["generator"]],
                "parent": row["parent"],
            }

        else:
            # bb.logit(f'Adding table data: {table}, {field}')
            if field not in tables[table]["fields"]:
                new_field = f'{field}   {ftype}'
                tables[table]["schema"].append(new_field)
                tables[table]["fields"].append(field)
                tables[table]["generator"].append(row["generator"])
        first = False
    ddl_post_actions(tables,table)
    clean_ddl(tables)
    bb.logit("Table DDL:")
    pprint.pprint(tables)
    #print("# ------------- TYPES ----------------------- #")
    #pprint.pprint(table_types(tables))
    sql_action(db_client, action, tables)
    return tables

def ddl_post_actions(tables, last_table):
    # Add modified to each table:
    closer = ")"
    seq_ddl = ""
    key_field = f"{last_table}_id".lower()
    new_field = f"modified_at    {gcp_type('timestamp')}"
    pair = last_table.split("_")
    tables[last_table]["schema"].append(new_field)
    closer = f" PRIMARY KEY ({key_field}))"  
    tables[last_table]["schema"].append(closer)
    tables[last_table]["fields"].append("modified_at")
    tables[last_table]["generator"].append("datetime.datetime.now()")

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
        # ddl = tables_obj[tab]["ddl"]
        # l = len(ddl)
        # ddl = ddl[:l-1] + ")"
        # tables_obj[tab]["ddl"] = ddl
        fmts = value_codes(tables_obj[tab]["fields"])
        tables_obj[tab][
            "insert"
        ] = f'insert into {tab} ({",".join(tables_obj[tab]["fields"])}) values ({fmts});'

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

# ----------------------------------------------------------------------#
#   Queries
# ----------------------------------------------------------------------#
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
    sql = "select m.*, p.nationalprovideridentifier, p.firstname, p.lastname, p.dateofbirth, p.gender from member m INNER JOIN provider p on p.provider_id = m.primaryprovider_id;"  # INNER JOIN providers p on m.primaryProvider_id = p.provider_id"
    result = sql_query("healthcare", sql)
    k = 0
    for item in result:
        if k < 20:
            pprint.pprint(item)
        k += 1


def get_claims(conn=""):
    conn = pg_connection()
    sql = "select * from claim"
    query_result = newsql_query(sql, conn)
    num_results = query_result["num_records"]
    print(f"Claims: {num_results}")
    ids = []
    for row in query_result["data"]:
        ids.append(row[1])
    result = get_claimlines(conn, ids)
    data = result["data"]
    inc = 0
    for k in data:
        print(f"#-------------------- {k} --------------------------")
        pprint.pprint(data[k])
        inc += 1
        if inc > 10:
            break
    conn.close()


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
    global settings
    instance_id = settings["spanner"]["instance_id"]
    database_id = settings["spanner"]["database_id"]
    numtodo = len(recs)
    cols = tuple(recs[0].keys())
    vals = []
    for it in recs:
        vals.append(tuple(it.values()))
    instance = nconn.instance(instance_id)
    database = instance.database(database_id)
    #pprint.pprint(cols)
    #if table.lower() == "quote_coverage_deductibleconditions":
    #    pprint.pprint(vals)
    bb.logit(f"Loading into Spanner: {len(recs)}")
    with database.batch() as batch:
        batch.insert(
            table=table,
            columns=cols,
            values=vals,
        )
    '''
    try:
        errors = nconn.insert_rows_json(table_id, recs)
        if errors == []:
            bb.logit(f"{len(recs)} inserted")
        else:
            bb.logit("Encountered errors while inserting rows: {}".format(errors))
    except Exception as err:
        bb.logit(f"{table} - {err}")
    '''


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

def test_spanner():
    sp_client = spanner_connection()
    bb.logit("# --- Check Dataset --- #")
    bb.logit("# --> Create Table")
    instance_id = settings["spanner"]["instance_id"]
    database_id = settings["spanner"]["database_id"]
    database_admin_api = sp_client.database_admin_api
    request = database_admin_api.UpdateDatabaseDdlRequest(
        database=database_admin_api.database_path(
            sp_client.project, instance_id, database_id
        ),
        statements=[sql],
    )
    operation = database_admin_api.update_database_ddl(request)

    bb.logit("Waiting for operation to complete...")
    operation.result(OPERATION_TIMEOUT_SECONDS)

    bb.logit("DDL - complete")

def sp_create_database_example(instance_id, database_id):
    """Creates a database and tables for sample data."""

    spanner_client = spanner.Client()
    database_admin_api = spanner_client.database_admin_api
    instance_id = settings["spanner"]["instance_id"]
    database_id = settings["spanner"]["database_id"]
    
    request = spanner_database_admin.CreateDatabaseRequest(
        parent=database_admin_api.instance_path(spanner_client.project, instance_id),
        create_statement=f"CREATE DATABASE `{database_id}`",
        extra_statements=[
            """CREATE TABLE Singers (
            SingerId     INT64 NOT NULL,
            FirstName    STRING(1024),
            LastName     STRING(1024),
            SingerInfo   BYTES(MAX),
            FullName   STRING(2048) AS (
                ARRAY_TO_STRING([FirstName, LastName], " ")
            ) STORED
        ) PRIMARY KEY (SingerId)""",
            """CREATE TABLE Albums (
            SingerId     INT64 NOT NULL,
            AlbumId      INT64 NOT NULL,
            AlbumTitle   STRING(MAX)
        ) PRIMARY KEY (SingerId, AlbumId),
        INTERLEAVE IN PARENT Singers ON DELETE CASCADE""",
        ],
    )

    operation = database_admin_api.create_database(request=request)

    print("Waiting for operation to complete...")
    database = operation.result(OPERATION_TIMEOUT_SECONDS)

    print(
        "Created database {} on instance {}".format(
            database.name,
            database_admin_api.instance_path(spanner_client.project, instance_id),
        )
    )

def gcp_type(mtype):
    type_pg = {
        "string": "character varying(255)",
        "boolean": "bool",
        "date": "date",
        "integer": "bigint",
        "real" : "real",
        "double": "numeric",
        "timestamp" : "timestamptz",
        "text" : "text"
    }
    type_bq = {
        "string": "STRING(255)",
        "boolean": "BOOL",
        "date": "DATE",
        "integer": "INT64",
        "double": "FLOAT64",
        "timestamp" : "TIMESTAMP",
        "text" : "STRING(MAX)"
    }
    ftype = type_pg[mtype.lower().strip()]
    return ftype

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
    mycon = spanner_connection()
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

def sql_action(conn, action, tables):
    if action == "none" or action == "info":
        return "no action"
    sql = ""
    for table_name in tables:
        if action == "create":
            schema = tables[table_name]["schema"]
            xtra = tables[table_name]["extra_ddl"]
            if xtra != "":
                result = spanner_ddl_action(conn, [xtra])
            result = spanner_ddl_action(conn, schema)
            
        elif action == "drop":
            conn.delete_table(tables[table_name]["table_id"], not_found_ok=True)
            print(f"Deleted: {table_name}")
        elif action == "delete":
            sql = f"delete from {table_name};"
    return "success"

def spanner_connection(type="spanner", sdb="none"):
    # export GOOGLE_APPLICATION_CREDENTIALS="/Users/brady.byrd/Documents/mongodb/dev/servers/gcp-pubsub-user/bradybyrd-poc-ac0790ea4120.json"
    client = spanner.Client()
    return client

def spanner_ddl_action(sp_client, sql):
    instance_id = settings["spanner"]["instance_id"]
    database_id = settings["spanner"]["database_id"]
    database_admin_api = sp_client.database_admin_api
    sql = ",\n".join(sql)
    sql = sql.replace("(,","(")
    bb.logit(f'Creating Table ddl')
    print(sql)
    request = spanner_database_admin.UpdateDatabaseDdlRequest(
        database=database_admin_api.database_path(
            sp_client.project, instance_id, database_id
        ),
        statements=[sql],
    )
    operation = database_admin_api.update_database_ddl(request)

    bb.logit("Waiting for operation to complete...")
    operation.result(OPERATION_TIMEOUT_SECONDS)

    bb.logit("DDL - complete")

def spanner_create_database(sp_client, db_name):
    instance_id = settings["spanner"]["instance_id"]
    db_name = settings["spanner"]["database_name"]
    database_admin_api = sp_client.database_admin_api
    request = database_admin_api.CreateDatabaseRequest(
        parent=database_admin_api.instance_path(sp_client.project, instance_id),
        create_statement=f"CREATE DATABASE `{db_name}`"
    )
    operation = database_admin_api.create_database(request=request)

    print("Waiting for operation to complete...")
    database = operation.result(OPERATION_TIMEOUT_SECONDS)

    print(
        "Created database {} on instance {}".format(
            database.name,
            database_admin_api.instance_path(sp_client.project, instance_id),
        )
    )
    bb.logit("Waiting for operation to complete...")
    operation.result(OPERATION_TIMEOUT_SECONDS)

    bb.logit("DDL - complete")


# ------------------------------------------------------------------#
#     MAIN
# ------------------------------------------------------------------#
settings_file = "relations_settings.json"
bb = Util()
ARGS = bb.process_args(sys.argv)
settings = bb.read_json(settings_file)

if __name__ == "__main__":
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
        load_spanner_data()
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
    elif ARGS["action"] == "create_database":
        spanner_create_database()
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
    elif ARGS["action"] == "test":
        test_big_query()
    else:
        print(f'{ARGS["action"]} not found')
    # conn.close()

"""
# ---------------------------------------------------- #

Create Database:
    python3 load_sql.py action=execute_ddl task=create
    python3 load_sql.py action=load_pg_data
"""
