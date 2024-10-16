import json
import logging
import sys
import time
from collections import OrderedDict, defaultdict
from ssl import SSLSocket
import datetime
from faker import Faker
from random import randint
import random
import pprint
import psycopg2
import redis
from bson.json_util import dumps
from bson.objectid import ObjectId
from pymongo import MongoClient

from bbutil import Util

faker = Faker()

settings_file = "../relations_settings.json"
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("faker").setLevel(logging.ERROR)

def generate_payments(num):
    payments = [None] * num
    for i in range(0, num):
        payments[i] = {}
        payments[i]["ApprovedAmount"] = randint(1, 10000)
        payments[i]["CoinsuranceAmount"] = randint(1, 10000)
        payments[i]["CopayAmount"] = randint(1, 1000)
        payments[i]["LatepaymentInterest"] = randint(1, 100)
        payments[i]["PaidAmount"] = randint(1, 100)
        payments[i]["PaidDate"] = faker.date()
        payments[i]["PatientPaidAmount"] = randint(1, 100)
        payments[i]["PatientResponsibilityAmount"] = randint(1, 100)
        payments[i]["PayerPaidAmount"] = randint(1, 100)
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
            match query:
                case "claim":  # Claim - only
                    SQL = "select *  from claim c where c.patient_id ='{}'".format(str(patient_id))
                case "claimLinePayments":  # Claim + Claimlines + Claimpayments
                    SQL = "select c.*, cl.* from claim c LEFT OUTER JOIN claim_claimline cl on cl.claim_id = c.claim_id where c.patient_id = '{}'".format(
                        str(patient_id)
                    )
                # Claim + Member + Provider (and a bunch of the sub tables)
                case "claimMemberProvider":
                    SQL = """select c.*, m.firstname, m.lastname, m.dateofbirth, m.gender, cl.*, ap.firstname as ap_first, ap.lastname as ap_last, ap.gender as ap_gender, ap.dateofbirth as ap_birthdate,
                                    op.firstname as op_first, op.lastname as op_last, op.gender as op_gender, op.dateofbirth as op_birthdate,
                                    rp.firstname as rp_first, rp.lastname as rp_last, rp.gender as rp_gender, rp.dateofbirth as rp_birthdate,
                                    opp.firstname as opp_first, opp.lastname as opp_last, opp.gender as opp_gender, opp.dateofbirth as opp_birthdate, ma.city as city, ma.state as us_state,
                                    mc.phonenumber as phone, mc.emailaddress as email
                                    from claim c
                                    INNER JOIN member m on m.member_id = c.patient_id
                                    LEFT OUTER JOIN claim_claimline cl on cl.claim_id = c.claim_id
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
            idnum += 1
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
    idnum = int(pair[1])
    for inc in range(iters):
        patient_id = f'{pair[0]}-{idnum}'
        instart = datetime.datetime.now()
        match query:
            case "claim":  # Claim - only
                result = claim.find({'Patient_id': patient_id})

            case "claimLinePayments":  # Claim + Claimlines + Claimpayments
                result = claim.find(
                    {'Patient_id': patient_id}, {'ClaimLine': 1})
            case "claimMemberProvider":  # Claim + Member + Provider
                result = claim.aggregate([
                    {
                        '$match': {
                            'Patient_id': patient_id
                        }
                    }, {
                        '$lookup': {
                            'from': 'member',
                            'localField': 'Patient_id',
                            'foreignField': 'Member_id',
                            'as': 'member'
                        }
                    }, {
                        '$lookup': {
                            'from': 'provider',
                            'localField': 'AttendingProvider_id',
                            'foreignField': 'Provider_id',
                            'as': 'provider'
                        }
                    }
                ])
        cnt = 0
        for data in result:
            # print(result)
            last_result = data
            cnt += 1
        timer(cnt,instart)
        idnum += 1
    pprint.pprint(last_result)
    timer(start,iters,"tot")

def transaction_mongodb(client, num_payment, manual=False):
    db = "healthcare"
    claim = client[db]["claim"]
    member = client[db]["member"]
    payment = generate_payments(num_payment)
    pipe = [{"$sample": {"size": num_payment}},
            {"$project": {"Claim_id": 1, "_id": 0}}]
    claim_ids = list(claim.aggregate(pipe))
    elapsed_transactions = 0
    for i in range(0, num_payment):
        claim_id = claim_ids[i]["Claim_id"]
        with client.start_session() as session:
            start = datetime.datetime.now()
            logging.debug(f"Transaction started for claim {claim_id}")
            with session.start_transaction():
                claim_update = claim.find_one_and_update(
                    {"Claim_id": claim_id},
                    {"$addToSet": {"Payment": payment[i]}},
                    projection={"Patient_id": 1},
                    session=session,
                )
                member_id = claim_update["Patient_id"]
                member.update_one(
                    {"Member_id": member_id},
                    {"$inc": {
                        "total_payments": payment[i]["PatientPaidAmount"]}},
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
            f"VALUES ('{pmt['claim_payment_id']}', '{pmt['claim_id']}', {payment[i]['ApprovedAmount']}, {payment[i]['CoinsuranceAmount']}, {payment[i]['CopayAmount']}, {payment[i]['LatepaymentInterest']}, {payment[i]['PaidAmount']}, '{payment[i]['PaidDate']}', {payment[i]['PatientPaidAmount']}, {payment[i]['PatientResponsibilityAmount']}, {payment[i]['PayerPaidAmount']}, now() );"
        )
        # claim + update total payment claim
        SQL_UPDATE_CLAIM = (
            f"UPDATE public.claim "
            f'SET totalpayments=  COALESCE(totalpayments ,0)  + {payment[i]["PatientPaidAmount"]} '
            f"WHERE claim_id = '{claim_id}';"
        )
        # FIND MEMBER ID
        member_id = sql_query(
            f"select patient_id from claim WHERE claim_id = '{claim_id}'", conn
        )
        member_id = member_id["data"][0][0]
        # members + update total payment
        SQL_UPDATE_MEMBER = (
            f"UPDATE public.member "
            f'SET totalpayments = COALESCE(totalpayments ,0) + {payment[i]["PatientPaidAmount"]} '
            f"WHERE member_id = '{member_id}';"
        )
        SQL_TRANSACTION = (
            f"BEGIN;"
            f"{SQL_INSERT} "
            f"{SQL_UPDATE_CLAIM} "
            f"{SQL_UPDATE_MEMBER} "
            f"COMMIT;"
        )
        # logging.debug(f"SQL Transaction : {SQL_TRANSACTION}")
        cur.execute(SQL_TRANSACTION)
        # conn.commit()

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
                {"$match" : {"claim_id" : new_id}},
                {"$lookup" : {
                    "from" : "member",
                    'localField': 'patient_id', 
                    'foreignField': 'member_id', 
                    'as': 'member_detail'
                    }
                }, 
                {'$unwind': {'path': '$member'}} 
            ]
        docs = mongodb[collection].aggregate(pipe)
        timer(instart)
    timer(start_time, iters, "tot")
    for doc in docs:
        pprint.pprint(docs)
        

# --------------------------------------------------------- #
#       UTILITY METHODS
# --------------------------------------------------------- #

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

def pg_connection(type="postgres", sdb="none"):
    shost = settings[type]["host"]
    susername = settings[type]["username"]
    spwd = settings[type]["password"]
    if sdb == "none":
        sdb = settings[type]["database"]
    conn = psycopg2.connect(host=shost, database=sdb,
                            user=susername, password=spwd)
    return conn

def redis_connection(type="redis_local"):
    rhost = settings[type]["host"]
    r = redis.Redis(host=rhost, port=6379, db=0)
    return r

def mongodb_connection(type="uri", details={}):
    mdb_conn = settings[type]
    username = settings["username"]
    password = settings["password"]
    if "username" in details:
        username = details["username"]
        password = details["password"]
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


# --------------------------------------------------------- #
#       MAIN
# --------------------------------------------------------- #
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    id_map = defaultdict(int)
    r_conn = None
    client = {"empty": True}
    conn = pg_connection()
    skip_cache = True
    skip_mongo = False
    iters = 1
    if not skip_mongo:
        client = mongodb_connection()
        mongodb = client["healthcare"]
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