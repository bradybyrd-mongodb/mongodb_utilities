import json
import logging
from ssl import SSLSocket
import sys
import time
import datetime
import multiprocessing
from collections import OrderedDict, defaultdict
import psycopg2
import redis
from bson.json_util import dumps
from bson.objectid import ObjectId
from pymongo import MongoClient

from bbutil import Util

settings_file = "../relations_settings.json"

logging.basicConfig(level=logging.DEBUG)

def get_claims_redis(r, key):
    try:
        r_data = r.get(key)
        if r_data is not None:
            json.loads(r_data)
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


def get_claims_sql(conn, query, patient_id, r):
    key = query + ":" + patient_id
    SQL = ""
    query_result = get_claims_redis(r, key)
    if query_result is not None:
        logging.debug(f"cache hit -> {patient_id}")
        print(query_result)
    else:
        match query:
            case "claim":  # Claim - only
                SQL = "select *  from claim c where c.patient_id ='{}'".format(
                    str(ARGS["patient_id"])
                )
            case "claimLinePayments":  # Claim + Claimlines + Claimpayments
                SQL = "select c.*, cl.* from claim c LEFT OUTER JOIN claim_claimline cl on cl.claim_id = c.claim_id where c.patient_id = '{}'".format(
                    str(ARGS["patient_id"])
                )
            case "claimMemberProvider":  # Claim + Member + Provider (and a bunch of the sub tables)
                SQL = """select c.*, m.firstname, m.lastname, m.dateofbirth, m.gender, cl.*, ap.firstname as ap_first, ap.lastname as ap_last, ap.gender as ap_gender, ap.dateofbirth as ap_birthdate, 
                                op.firstname as op_first, op.lastname as op_last, op.gender as op_gender, op.dateofbirth as op_birthdate, 
                                rp.firstname as rp_first, rp.lastname as rp_last, rp.gender as rp_gender, rp.dateofbirth as rp_birthdate, 
                                opp.firstname as opp_first, opp.lastname as opp_last, opp.gender as opp_gender, opp.dateofbirth as opp_birthdate, ma.city as city, ma.state as us_state, 
                                mc.phonenumber as phone, mc.emailaddress as email, me.employeeidentificationnumber as EIN
                                from claim c  
                                INNER JOIN member m on m.member_id = c.patient_id 
                                LEFT OUTER JOIN claim_claimline cl on cl.claim_id = c.claim_id 
                                INNER JOIN provider ap on cl.attendingprovider_id = ap.provider_id
                                INNER JOIN provider op on cl.orderingprovider_id = op.provider_id 
                                INNER JOIN provider rp on cl.referringprovider_id = rp.provider_id 
                                INNER JOIN provider opp on cl.operatingprovider_id = opp.provider_id 
                                LEFT JOIN member_address ma on ma.member_id = m.member_id 
                                INNER JOIN (select * from member_communication where emailtype = 'Work' and member_id = 'M-2030000' limit 1) mc on mc.member_id = m.member_id
                                LEFT JOIN member_languages ml on ml.member_id = m.member_id 
                                LEFT JOIN member_employment me on me.member_id = m.member_id 
                                INNER JOIN member_disability md on md.member_id = m.member_id 
                                INNER JOIN member_guardian mg on mg.member_id = m.member_id 
                                where c.patient_id = '{}' and ma.name = 'Main' """.format(
                    str(ARGS["patient_id"])
                )
        logging.debug(f"cache miss -> {patient_id}")
        logging.debug(f"fetching from psql {SQL}")
        query_result = newsql_query(SQL, conn)
        load_claims_redis(r, key, query_result)
        num_results = query_result["num_records"]
        logging.debug(f"found {num_results} records")

        print(f"Claims: {num_results}")
        ids = []
        for row in query_result["data"]:
            print(row)

def add_payment(sql, conn):
    # add a record to claims_payment table
    cur = conn.cursor()
    cdate = datetime.datetime.now()
    pmt = {"claim_id":"","claim_payment_id":"","approvedamount":0,"coinsuranceamount":0,"paidamount":0,"paiddate": cdate,"patientresponsibilityamount":0}
    pmtid = 3000000
    for iter in range(10):
        year = 2023
        month = 7 - random.randint(1,6)
        day = random.randint(1,28)
        idnum = random.randint(1000000,1050000)
        pmt["claim_id"] = f"C-{idnum}"
        pmt["claim_payment_id"] = f"CP-{pmtid}"
        pmt["approvedamouth"] = random.randint(20,100)
        pmt["paidamount"] = random.randint(20,100)
        pmt["paiddate"] = datetime.datetime(year,month,day, 10, 45)
        vals = ""
        for it in pmt:
            
        sql = f"insert into claim_payment ({','.join(list(pmt.keys()))}), values ({"vals"})"
        sql_execute(sql)
        pmtid += 1
    cur.close()

def sql_execute(sql):
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except psycopg2.DatabaseError as err:
        print(f"{sql} - {err}")
    cur.close()

def newsql_query(sql, conn):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        row_count = cur.rowcount
        print(f"{row_count} records")
        result = {"num_records": row_count, "data": cur.fetchall()}
        return result
    except psycopg2.DatabaseError as err:
        print(f"{sql} - {err}")
    cur.close()

# --------------------------------------------------------- #
#       UTILITY METHODS
# --------------------------------------------------------- #
def timer(t_start, quiet = True):
    #  Reads file and finds values
    end_time = datetime.datetime.now()
    time_diff = (end_time - t_start)
    execution_time = time_diff.total_seconds() + time_diff.microseconds * .000001
    if not quiet:
        cur_process = multiprocessing.current_process()
        procid = cur_process.name.replace("process", "p")
        print(f"{procid} - Bulk Load took {'{:.3f}'.format(execution_time)} seconds")
    return execution_time

def pg_connection(type="postgres", sdb="none"):
    shost = settings[type]["host"]
    susername = settings[type]["username"]
    spwd = settings[type]["password"]
    if sdb == "none":
        sdb = settings[type]["database"]
    conn = psycopg2.connect(host=shost, database=sdb, user=susername, password=spwd)
    return conn


def redis_connection(type="redis_local"):
    rhost = settings[type]["host"]
    r = redis.Redis(host=rhost, port=6379, db=0)
    return r

if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    base_counter = settings["base_counter"]
    id_map = defaultdict(int)
    conn = pg_connection()
    r = redis_connection()
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "get_claims_sql":
        if "patient_id" not in ARGS:
            print(
                "Send patient_id= argument e.g: python3 gcp_getclaimlines.py action=get_claims_sql patient_id='M-2030000' query=claim or claimLinePayments or  claimMemberProvider "
            )
            sys.exit(1)

        if ARGS["query"] not in [
            "claim",
            "claimLinePayments",
            "claimMemberProvider",
        ]:
            print()
            print(
                "Send patient_id= argument e.g: python3 gcp_getclaimlines.py action=get_claims_sql patient_id='M-2030000' query=claim or claimLinePayments or  claimMemberProvider "
            )
            sys.exit(1)

        else:
            get_claims_sql(conn, ARGS["query"], ARGS["patient_id"], r)

    # elif ARGS["action"] == "get_claimlines_mdb":
    #     get_claimlines_mdb()
    else:
        print(f'{ARGS["action"]} not found')

    conn.close()
    r.close()
    