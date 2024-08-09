import json
import logging
import sys
import os
import string
import time
import multiprocessing
from collections import OrderedDict, defaultdict
from ssl import SSLSocket
import datetime
from faker import Faker
from random import randint
import random
import pprint
import pymongo
import psycopg2
#import redis
from bson.json_util import dumps
from bson.objectid import ObjectId
from pymongo import MongoClient
# -------- Spanner ---------- #
from google.cloud import spanner, spanner_admin_database_v1
from google.cloud.spanner_admin_database_v1.types.common import DatabaseDialect
from google.cloud.spanner_v1 import param_types
from google.cloud.spanner_v1.data_types import JsonObject
from google.cloud.spanner_admin_database_v1.types import spanner_database_admin

from bbutil import Util

faker = Faker()
letters = string.ascii_uppercase
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("faker").setLevel(logging.ERROR)
base_dir = os.path.dirname(os.path.abspath(__file__))
'''
# ----------------------------------------------------------- #
#    Transactions Compare Postgres and MongoDB
remember to set - _PWD_ and _PGPWD_ on command line
Demo instructions in: transaction_demoscript.md


'''
def generate_payments(num):
    payments = [None] * num
    for i in range(0, num):
        payments[i] = {}
        payments[i]["approvedAmount"] = randint(1, 10000)
        payments[i]["coinsuranceAmount"] = randint(1, 10000)
        payments[i]["copayAmount"] = randint(1, 1000)
        payments[i]["latepaymentInterest"] = randint(1, 100)
        payments[i]["paidAmount"] = randint(1, 100)
        payments[i]["paidDate"] = faker.date()
        payments[i]["patientPaidAmount"] = randint(1, 100)
        payments[i]["patientResponsibilityAmount"] = randint(1, 100)
        payments[i]["payerPaidAmount"] = randint(1, 100)
    logging.debug(f"Payment objects generated")
    return payments


def sql_execute(sql):
    cur = conn.cursor()
    try:
        sql = cur.execute(sql)
    except psycopg2.DatabaseError as err:
        print(f"{sql} - {err}")
    cur.close()


def get_claims_redis(r, key):
    try:
        r_data = r.get(key)
        if r_data is not None:
            r_data = json.loads(r_data)
        return r_data
    except Exception as error:
        logging.error(f" {error}")


def load_claims_redis(r, key, data):
    try:
        load_state = r.set(key, json.dumps(data, default=dumps))
        logging.debug(f"loaded to redis status {load_state}")
        if load_state is True:
            logging.debug(f"claim loaded to redis for claim {key}")
        return load_state
    except Exception as error:
        logging.error(f" {error}")


def get_claims_sql(conn, query, patient_id, r, skip_cache, iters = 1):
    start = datetime.datetime.now()
    SQL = ""
    query_result = None
    increment = 1
    if "inc" in ARGS:
        increment = int(ARGS["inc"])
    cache_hit = False
    pair = patient_id.split("-")
    idnum = int(pair[1])
    for inc in range(iters):
        patient_id = f'{pair[0]}-{idnum}'
        instart = datetime.datetime.now()
        if not skip_cache:
            key = query + ":" + patient_id
            query_result = get_claims_redis(r, key)
            if query_result is not None:
                logging.debug(f"cache hit -> {patient_id}")
                print(query_result)
                cache_hit = True
            else:
                logging.debug(f"cache miss -> {patient_id}")
                logging.debug(f"fetching from psql {SQL}")
        if not cache_hit:            
            if query == "claim":  # Claim - only
                SQL = "select *  from claim c where c.patient_id ='{}'".format(str(patient_id))
            elif query == "claimLinePayments":  # Claim + Claimlines + Claimpayments
                SQL = "select c.*, cl.*, cp.* from claim c LEFT JOIN claim_payment cp on cp.claim_id = c.claim_id LEFT OUTER JOIN claim_claimline cl on cl.claim_id = c.claim_id where c.patient_id = '{}'".format(
                    str(patient_id)
                )
                # Claim + Member + Provider (and a bunch of the sub tables)
            elif query == "claimMemberProvider":
                SQL = """select c.*, m.firstname, m.lastname, m.dateofbirth, m.gender, cl.*, cp.paidamount, cp.paiddate, ap.firstname as ap_first, ap.lastname as ap_last, ap.gender as ap_gender, ap.dateofbirth as ap_birthdate,
                                op.firstname as op_first, op.lastname as op_last, op.gender as op_gender, op.dateofbirth as op_birthdate,
                                rp.firstname as rp_first, rp.lastname as rp_last, rp.gender as rp_gender, rp.dateofbirth as rp_birthdate,
                                opp.firstname as opp_first, opp.lastname as opp_last, opp.gender as opp_gender, opp.dateofbirth as opp_birthdate, ma.city as city, ma.state as us_state,
                                mc.phonenumber as phone, mc.emailaddress as email
                                from claim c
                                INNER JOIN member m on m.member_id = c.patient_id
                                LEFT OUTER JOIN claim_claimline cl on cl.claim_id = c.claim_id
                                LEFT JOIN claim_payment cp on cp.claim_id = c.claim_id
                                INNER JOIN provider ap on cl.attendingprovider_id = ap.provider_id
                                INNER JOIN provider op on cl.orderingprovider_id = op.provider_id
                                INNER JOIN provider rp on cl.referringprovider_id = rp.provider_id
                                INNER JOIN provider opp on cl.operatingprovider_id = opp.provider_id
                                LEFT JOIN (select * from member_address where type = 'Main' limit 1) ma on ma.member_id = m.member_id
                                INNER JOIN (select * from member_communication where emailtype = 'Work' and member_id = '{}' limit 1) mc on mc.member_id = m.member_id
                                where c.patient_id = '{}' """.format(
                                                    str(patient_id), str(patient_id)
                )
            query_result = sql_query(SQL, conn)
            if not skip_cache:
                load_claims_redis(r, key, query_result)
            #num_results = query_result["num_records"]
            #logging.debug(f"found {num_results} records")
            cnt = 0
            for data in query_result["data"]:
                # print(result)
                cnt += 1
            timer(instart, cnt)
            idnum += increment
    #logging.debug(f"records fetched from psql")
    logging.debug("# --------------------- SQL --------------------------- #")
    logging.debug(SQL)
    timer(start,iters,"tot")

def get_claims_mongodb(client, query, patient_id, iters = 1):
    claim = client["claim"]
    result = ''
    last_result = {}
    start = datetime.datetime.now()
    pair = patient_id.split("-")
    pipe = {}
    idnum = int(pair[1])
    for inc in range(iters):
        patient_id = f'{pair[0]}-{idnum}'
        instart = datetime.datetime.now()
        if query == "claim":  # Claim - only
            pipe = {'patient_id': patient_id}
            result = claim.find(pipe)
        elif query == "claimLinePayments":  # Claim + Claimlines + Claimpayments
            pipe = {'patient_id': patient_id}
            result = claim.find(
                {'patient_id': patient_id}, {'claimLine': 1})
        elif query == "claimMemberProvider":  # Claim + Member + Provider
            pipe = [
                {
                    '$match': {
                        'patient_id': patient_id
                    }
                }, {
                    '$lookup': {
                        'from': 'member',
                        'localField': 'patient_id',
                        'foreignField': 'member_id',
                        'as': 'member'
                    }
                },
                {
                    "$unwind" : {"path" : "$member"}
                }, {
                    '$lookup': {
                        'from': 'provider',
                        'localField': 'attendingProvider_id',
                        'foreignField': 'provider_id',
                        'as': 'provider'
                    }
                },
                {
                    "$unwind" : {"path" : "$provider"}
                }
            ]
            result = claim.aggregate(pipe)
        cnt = 0
        for data in result:
            #pprint.pprint(data)
            last_result = data
            cnt += 1
        timer(instart,cnt)
        idnum += 1
    print("# ---------------- Sample Document ---------------------- #")
    pprint.pprint(last_result)
    print("# ---------------- Pipeline ---------------------- #")
    pprint.pprint(pipe)
    timer(start,iters,"tot")

def transaction_mongodb(client, num_payment, manual=False):
    db = settings["database"]
    claim = client[db]["claim"]
    member = client[db]["member"]
    payment = generate_payments(num_payment)
    pipe = [{"$sample": {"size": num_payment}},
            {"$project": {"claim_id": 1, "_id": 0}}]
    claim_ids = list(claim.aggregate(pipe))
    elapsed_transactions = 0
    for i in range(0, num_payment):
        claim_id = claim_ids[i]["claim_id"]
        with client.start_session() as session:
            start = datetime.datetime.now()
            logging.debug(f"Transaction started for claim {claim_id}")
            with session.start_transaction():
                claim_update = claim.find_one_and_update(
                    {"claim_id": claim_id},
                    {"$addToSet": {"payment": payment[i]}},
                    projection={"patient_id": 1, "payment" : 1},
                    session=session,
                )
                member_id = claim_update["patient_id"]
                tot = 0
                #print("# ---------- Claim Update ----------- #")
                #pprint.pprint(claim_update)
                for it in claim_update["payment"]:
                    tot += it["patientPaidAmount"]
                member.update_one(
                    {"member_id": member_id},
                    #{"$inc": {
                    #    "total_payments": payment[i]["patientPaidAmount"]}},
                    {"$set" : {
                        "total_payments" : tot
                    }},
                    session=session,
                )
                if manual:
                    print(f'Enter "yes" to commit or anything to abort transaction')
                    abort = input()
                    if abort != "yes":
                        logging.debug(f"Transaction aborted: {abort}")
                        raise Exception("Operation aborted")
                # switch to abort transaction.
                session.commit_transaction()
                elapsed = datetime.datetime.now() - start
                logging.debug(
                    f"Transaction took: {'{:.3f}'.format(elapsed.microseconds / 1000)} ms")
                logging.debug(f"Transaction completed")
                elapsed_transactions += elapsed.microseconds
    logging.debug(
        f"Transaction average time took for {num_payment} transactions: {'{:.3f}'.format(elapsed_transactions / (num_payment * 1000))} ms"
    )
    logging.debug(f"Test completed")


# add mongodb query performance


def transaction_postgres(conn, num_payment):
    payment = generate_payments(num_payment)
    conn.autocommit = False #for multi-line transactions
    cur = conn.cursor()

    SQL_RANDOM = "select claim_id from claim order by random() limit {};".format(
        num_payment
    )
    claim_ids = sql_query(SQL_RANDOM, conn)
    elapsed_transactions = 0
    for i in range(0, num_payment):
        start = datetime.datetime.now()
        claim_id = claim_ids["data"][i][0]
        logging.debug(f"SQL Transaction started for claim {claim_id}")
        pmt = {
            "claim_id": claim_id,
            "claim_payment_id": "",
            "approvedamount": 0,
            "coinsuranceamount": 0,
            "paidamount": 0,
            "patientresponsibilityamount": 0,
        }
        pmtid = 3000000
        year = 2023
        month = 7 - random.randint(1, 6)
        day = random.randint(1, 28)
        idnum = random.randint(1000000, 1050000)
        pmt["claim_payment_id"] = f"CP-{pmtid}"
        pmt["approvedamouth"] = random.randint(20, 100)
        pmt["paidamount"] = random.randint(20, 100)
        pmt["paiddate"] = datetime.datetime(year, month, day, 10, 45)
        # claim payment + insert new payment claim
        SQL_INSERT = (
            f"INSERT INTO claim_payment(claim_payment_id, claim_id, approvedamount, coinsuranceamount, copayamount, latepaymentinterest, paidamount, paiddate, patientpaidamount, patientresponsibilityamount, payerpaidamount, modified_at)"
            f"VALUES ('{pmt['claim_payment_id']}', '{pmt['claim_id']}', {payment[i]['approvedAmount']}, {payment[i]['coinsuranceAmount']}, {payment[i]['copayAmount']}, {payment[i]['latepaymentInterest']}, {payment[i]['paidAmount']}, '{payment[i]['paidDate']}', {payment[i]['patientPaidAmount']}, {payment[i]['patientResponsibilityAmount']}, {payment[i]['payerPaidAmount']}, now() );"
        )
        cur.execute(SQL_INSERT)
        # claim + update total payment claim
        SQL_TALLY_PAYMENTS = (
            f"select claim_id, sum(patientpaidamount) as tot_payments from claim_payment "
            f"where claim_id = '{claim_id}' "
            f"group by claim_id;"
        )
        cur.execute(SQL_TALLY_PAYMENTS)
        rows = cur.fetchall()
        tot = 0
        for i in rows:
            tot += i[1]
        # claim + update total payment claim
        SQL_UPDATE_CLAIM = (
            f"UPDATE public.claim "
            f'SET totalpayments=  {tot} ' #COALESCE(totalpayments ,0)  + {payment[i]["patientPaidAmount"]} '
            f"WHERE claim_id = '{claim_id}';"
        )
        cur.execute(SQL_UPDATE_CLAIM)
        # FIND MEMBER ID
        member_id = sql_query(
            f"select patient_id from claim WHERE claim_id = '{claim_id}'", conn
        )
        member_id = member_id["data"][0][0]
        # members + update total payment
        SQL_UPDATE_MEMBER = (
            f"UPDATE public.member "
            f'SET totalpayments = {tot} ' #COALESCE(totalpayments ,0) + {payment[i]["patientPaidAmount"]} '
            f"WHERE member_id = '{member_id}';"
        )
        cur.execute(SQL_UPDATE_MEMBER)
        
        conn.commit()

        elapsed = datetime.datetime.now() - start
        logging.debug(f"Transaction took: {'{:.3f}'.format(elapsed.microseconds / 1000)} ms")
        logging.debug(f"Transaction completed")
        elapsed_transactions += elapsed.microseconds
    logging.debug(
        f"Transaction average time took for {num_payment} transactions: {'{:.3f}'.format(elapsed_transactions / (num_payment * 1000))} ms"
    )
    logging.debug(f"Test completed")

# --------------------------------------------------------- #
#    API Presentation
# --------------------------------------------------------- #

#  SQL Query - 7 tables queried, looping through 60+ records in 7 cursors
#    Additional 9 tables to get member information

# GET api/v1/claim?claim_id=<id>
def get_claim_api_sql(conn, claim_id, add_member = False):
    iters = 1
    if "iters" in ARGS:
        iters = int(ARGS["iters"])
    base_id = int(claim_id.replace("C-",""))
    start_time = datetime.datetime.now()
    rich = ""
    if add_member:
        rich = "Rich "
    bb.message_box(f"{rich}Claim API - MongoDB","title")
    for k in range(iters):
        instart = datetime.datetime.now()
        new_id = f'C-{base_id + k}'
        result = {}
        cursor = conn.cursor()
        tables = ['claim_claimline',
                'claim_diagnosiscode',
                'claim_notes',
                'claim_payment'
                ]
        sub_tables = ['claim_claimline_payment',
                'claim_claimline_diagnosiscodes'
                ]
        sub_key = "claim_claimline_id"
        primary_table = "claim"
        claim_sql = f'select * from {primary_table} where claim_id = \'{new_id}\' limit(1)'
        answer = sql_query(claim_sql, conn)
        #bb.logit(f'Primary table: {primary_table} - {answer["num_records"]}')
        recs = answer["data"]
        result = jsonize_records(conn, "claim", recs)[0]
            
        for tab in tables:
            claim_sql = f'select * from {tab} where claim_id = \'{new_id}\''
            answer = sql_query(claim_sql, conn)
            #bb.logit(f'Related table: {tab} - {answer["num_records"]}')
            recs = jsonize_records(conn, tab, answer["data"])
            if tab in sub_tables[0]:
                for subtab in sub_tables:
                    icnt = 0
                    for it in recs:
                        sub_claim_sql = f'select * from {subtab} where {sub_key} = \'{it[sub_key]}\''
                        subanswer = sql_query(sub_claim_sql, conn)
                        #bb.logit(f'Related subtable: {subtab} - {subanswer["num_records"]}')
                        subrecs = jsonize_records(conn, subtab, subanswer["data"])
                        recs[icnt][subtab] = subrecs
                        icnt += 1
            result[tab] = recs
        if add_member:
            member = add_member_info_sql(conn, result["patient_id"])
            result["member_detail"] = member
        timer(instart)
    timer(start_time, iters, "tot")
    pprint.pprint(result)
    
def jsonize_records(conn, table, results):
    result = []
    cols = column_names(table, conn)
    icnt = 0
    for row in results:
        rec = {}
        icnt = 0
        for col in row:
            rec[cols[icnt]] = col
            icnt += 1
        result.append(rec)
    return result

def add_member_info_sql(conn, member_id):
    primary_table = "member"
    tables = ['member_address',
              'member_bankaccount',
              'member_communication',
              'member_disability',
              'member_employment',
              'member_guardian',
              'member_languages'
              ]
    sql = f'select * from {primary_table} where member_id = \'{member_id}\' limit(1)'
    answer = sql_query(sql, conn)
    #bb.logit(f'Primary table: {primary_table} - {answer["num_records"]}')
    recs = answer["data"]
    result = jsonize_records(conn, "member", recs)[0]        
    for tab in tables:
        claim_sql = f'select * from {tab} where member_id = \'{member_id}\''
        answer = sql_query(claim_sql, conn)
        #bb.logit(f'Related table: {tab} - {answer["num_records"]}')
        recs = jsonize_records(conn, tab, answer["data"])
        result[tab] = recs
    return result

# MongoDB - single query, 1 IOP, single document returned    
def get_claim_api(client, claim_id):
    iters = 1
    if "iters" in ARGS:
        iters = int(ARGS["iters"])
    mongodb = client["healthcare"]
    collection = "claim"
    bb.message_box("Claim API - MongoDB","title")
    base_id = int(claim_id.replace("C-",""))
    start_time = datetime.datetime.now()
    for k in range(iters):
        instart = datetime.datetime.now()
        new_id = f'C-{base_id + k}'
        docs = mongodb[collection].find_one({"Claim_id" : new_id})
        timer(instart)
    timer(start_time, iters, "tot")
    pprint.pprint(docs)

def get_claim_api_rich(client, claim_id):
    iters = 1
    if "iters" in ARGS:
        iters = int(ARGS["iters"])
    mongodb = client["healthcare"]
    collection = "claim"
    bb.message_box("Rich Claim API - MongoDB","title")
    base_id = int(claim_id.replace("C-",""))
    start_time = datetime.datetime.now()
    for k in range(iters):
        instart = datetime.datetime.now()
        new_id = f'C-{base_id + k}'
        pipe = [
                {"$match" : {"Claim_id" : new_id}},
                {"$lookup" : {
                    "from" : "member",
                    'localField': 'Patient_id', 
                    'foreignField': 'Member_id', 
                    'as': 'member_detail'
                    }
                }, 
                {'$unwind': {'path': '$member_detail'}} 
            ]
        docs = mongodb[collection].aggregate(pipe)
        timer(instart)
    timer(start_time, iters, "tot")
    for doc in docs:
        pprint.pprint(docs)
        
# --------------------------------------------------------- #
#       SQL MIGRATION METHODS
# --------------------------------------------------------- #
def sql_migration():
    #loop through a drirectory of sql scripts and execute in order
    path = f'{base_dir}/mainframe_offload/sql_migration'
    bb.message_box("Performing SQL v2.0 Migration","title")
    if "path" in ARGS:
        path = ARGS["path"]
    cur = conn.cursor()
    cnt = 0
    tot = 0
    
    files = os.scandir(path) #os.walk(path, topdown=True):
    conts = ""
    cnt = 0
    bb.logit(f'# {path}')
    #dir_doc = file_info(root, "dir")
    filelist = []
    for fil in files:
        if fil.is_dir():
            continue
        if fil.name.startswith("."):
            continue
        if fil.name.endswith(".sql"):
            filelist.append(fil.name)
        cnt += 1
        #bb.logit(fil.name)
        
    bb.logit("#--------------------------------------------------#")
    filelist.sort()
    for fil in filelist:    
        cur_file = os.path.join(path,fil)
        bb.logit(f'Executing script: {fil}')
        with open (cur_file , "r") as fil:
            conts = fil.read()
            print("# ---------------------------------------------- #")
            print(conts)            
        try:
            res = cur.execute(conts)
            conn.commit()
        except Exception as e:
            print(e)
            print(e.pgerror)
        tot += 1
    
    bb.logit(f'Completed {tot} directory items')
    conn.close()

# --------------------------------------------------------- #
#    MongoDB Migration to v2 policy
# --------------------------------------------------------- #
#  add policy coverage subdocument
def migrate_product_coverage(db = None):
    collection = "policy"
    bb.message_box("Performing v2.0 Migration","title")
    pipe = [
        {
            '$lookup': {
                'from': 'product', 
                'localField': 'product_id', 
                'foreignField': 'product_id', 
                'pipeline': [{'$project': {'coverage': 1, '_id': 0}}], 
                'as': 'product'
            }
        }, {
            '$unwind': {'path': '$product'}
        }, {
            '$project': {
                'policy_id': 1, 
                'plan_sponsor_id': 1, 
                'name': 1, 
                'effectiveDate': 1, 
                'expirationDate': 1, 
                'coverage': '$product.coverage', 
                'version': 1, 
                'isActive': 1, 
                'premium': 1,
                'holder' : 1,
                'doc_version': 1
            }
        }, {
            '$out': collection
        }
    ]
    bb.logit("Aggregating product coverage...")
    db[collection].rename("policy_temp", dropTarget=False)
    db.policy_temp.aggregate(pipe)
    bb.logit("Creating indexes...")
    db.policy.create_index([("policy_id" , pymongo.ASCENDING)])
    db.policy.create_index([("product_id" , pymongo.ASCENDING)])
    db.policy.create_index([("holder.member_id" , pymongo.ASCENDING)])
    add_version(db)

def add_version(db = None):
    db.product.update_many({},{"$set": {"version" : 
                                        {"name" : "1.0",
                                         "start_date" : datetime.datetime.now(),
                                         "end_date" : datetime.datetime.now(),
                                         "is_active" : True}}})
    db.policy.update_many({},{"$set": {"version" : "1.0"}})
    db.product.create_index([("version.name", pymongo.ASCENDING)])

# --------------------------------------------------------- #
#       UTILITY METHODS
# --------------------------------------------------------- #
def file_info(file_obj, type = "file"):
    try:
        file_stats = os.stat(file_obj)
        doc = OrderedDict()
        doc["path_raw"] = file_obj
        if type == "file":
            doc["is_object"] = True
            doc["num_objects"] = 0
            doc["size_kb"] = file_stats.st_size * .001
        else:
            doc["is_object"] = False
            doc["num_objects"] = 0
            doc["size_kb"] = 0
        doc["permissions"] = file_stats.st_mode
        doc["owner"] = f'{file_stats.st_uid}:{file_stats.st_gid}'
        m_at = file_stats.st_mtime
        c_at = file_stats.st_ctime
        doc["modified_at"] = datetime.fromtimestamp(m_at).strftime('%Y-%m-%d %H:%M:%S')      
        doc["created_at"] = datetime.fromtimestamp(c_at).strftime('%Y-%m-%d %H:%M:%S') 
        doc["paths"] = file_obj.split("/")
    except:
        bb.logit(f'Path: {file_obj} inaccessible')
        doc = {"error" : True}
    return(doc)

def timer(starttime,cnt = 1, ttype = "sub"):
    elapsed = datetime.datetime.now() - starttime
    secs = elapsed.seconds
    msecs = elapsed.microseconds
    if secs == 0:
        elapsed = msecs * .001
        unit = "ms"
    else:
        elapsed = secs + (msecs * .000001)
        unit = "s"
    if ttype == "sub":
        logging.debug(f"query ({cnt} recs) took: {'{:.3f}'.format(elapsed)} {unit}")
    else:
        logging.debug(f"# --- Complete: query took: {'{:.3f}'.format(elapsed)} {unit} ---- #")
        logging.debug(f"#   {cnt} items {'{:.3f}'.format((elapsed)/cnt)} {unit} avg")

def column_names(table, conn):
    sql = f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = '{table}'"
    cur = conn.cursor()
    # print(sql)
    try:
        cur.execute(sql)
        row_count = cur.rowcount
        #print(f"{row_count} columns")
    except psycopg2.DatabaseError as err:
        print(f"{sql} - {err}")
    rows = cur.fetchall()
    result = []
    for i in rows:
        result.append(i[0])
    cur.close()
    return result

#Spanner Additions
def spanner_connection(type="spanner", sdb="none"):
    # export GOOGLE_APPLICATION_CREDENTIALS="/Users/brady.byrd/Documents/mongodb/dev/servers/gcp-pubsub-user/bradybyrd-poc-ac0790ea4120.json"
    client = spanner.Client()
    return client

def pg_connection(type="postgres", sdb="none"):
    shost = settings[type]["host"]
    susername = settings[type]["username"]
    spwd = settings[type]["password"]
    if "secret" in spwd:
        spwd = os.environ.get("_PGPWD_")
    if sdb == "none":
        sdb = settings[type]["database"]
    conn = psycopg2.connect(host=shost, database=sdb,
                            user=susername, password=spwd)
    return conn

def redis_connection(type="redis_local"):
    rhost = settings[type]["host"]
    #r = redis.Redis(host=rhost, port=6379, db=0)
    return r

def mongodb_connection(type="uri", details={}):
    mdb_conn = settings[type]
    username = settings["username"]
    password = settings["password"]
    if "username" in details:
        username = details["username"]
        password = details["password"]
    if "secret" in password:
        password = os.environ.get("_PWD_")
    mdb_conn = mdb_conn.replace("//", f"//{username}:{password}@")
    logging.debug(f"Connecting: {mdb_conn}")
    if "readPreference" in details:
        client = MongoClient(
            mdb_conn, readPreference=details["readPreference"]
        )  # &w=majority
    else:
        client = MongoClient(mdb_conn)
    return client

def sql_query(sql, conn):
    return postgres_query(conn,sql) if platform == "postgres" else spanner_query(conn,sql)
        

def postgres_query(conn, sql):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        row_count = cur.rowcount
        #print(f"{row_count} records")
        rows = cur.fetchall()
        result = {"num_records": row_count, "data": rows}
        return result
    except psycopg2.DatabaseError as err:
        print(f"{sql} - {err}")
    cur.close()

def spanner_query(conn, sql):
    # Queries sample data from spanner using SQL.
    instance_id = settings["spanner"]["instance_id"]
    database_id = settings["spanner"]["database_id"]
    instance = conn.instance(instance_id)
    database = instance.database(database_id)
    result = {"num_records": 0, "data": []}
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            sql
        )
        #for row in results:
            #print("SingerId: {}, AlbumId: {}, AlbumTitle: {}".format(*row))
        #print(sql)
        result = {"num_records": 0, "data": results}    
    return result


# --------------------------------------------------------- #
#       MAIN
# --------------------------------------------------------- #
if __name__ == "__main__":
    settings_file = "relations_settings.json"
    platform = "postgres" #"spanner" #
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    id_map = defaultdict(int)
    r_conn = None
    client = {"empty": True}
    conn = pg_connection()
    #conn = spanner_connection()
    skip_cache = True
    skip_mongo = False
    iters = 1
    if not skip_mongo:
        client = mongodb_connection()
        mongodb = client[settings["database"]]
    if "cache" in ARGS:
        skip_cache = ARGS["cache"].lower() == 'false'
    if not skip_cache:
        r_conn = redis_connection()
    if "iters" in ARGS:
        iters = int(ARGS["iters"])
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "get_claims_sql":
        if "cache" in ARGS:
            ARGS["cache"].lower() == 'true'
            use_cache = True
        if "patient_id" not in ARGS:
            print("Send patient_id= argument e.g: python3 gcp_getclaimlines.py action=get_claims_sql patient_id='M-2030000' query=claim or claimLinePayments or  claimMemberProvider ")
            sys.exit(1)
        if ARGS["query"] not in ["claim", "claimLinePayments", "claimMemberProvider"]:
            print("Send patient_id= argument e.g: python3 gcp_getclaimlines.py action=get_claims_sql patient_id='M-2030000' query=claim or claimLinePayments or  claimMemberProvider ")
            sys.exit(1)
        else:
            get_claims_sql(conn, ARGS["query"], ARGS["patient_id"], r_conn, skip_cache, iters)
    elif ARGS["action"] == "get_claims_mongodb":
        get_claims_mongodb(mongodb, ARGS["query"], ARGS["patient_id"], iters)
    elif ARGS["action"] == "db_migrate":
        sql_migration()
    elif ARGS["action"] == "mdb_migrate":
        migrate_product_coverage(mongodb)
    elif ARGS["action"] == "transaction_mongodb":
        mcommit = False
        if "mcommit" in ARGS:
            mcommit = ARGS["mcommit"].lower() == 'true'
        transaction_mongodb(client, int(ARGS["num_transactions"]), mcommit)
    elif ARGS["action"] == "transaction_postgres":
        transaction_postgres(conn, int(ARGS["num_transactions"]))
    elif ARGS["action"] == "get_claim_api_sql":
        claim_id = ARGS["claim_id"]
        rich = False
        if "rich" in ARGS:
            rich = True
        get_claim_api_sql(conn, claim_id, rich)
    elif ARGS["action"] == "get_claim_api":
        claim_id = ARGS["claim_id"]
        if "rich" in ARGS:
            get_claim_api_rich(client, claim_id)
        else:
            get_claim_api(client, claim_id)
    else:
        print(f'{ARGS["action"]} not found')

    conn.close()
    if not skip_cache:
        r_conn.close()
    