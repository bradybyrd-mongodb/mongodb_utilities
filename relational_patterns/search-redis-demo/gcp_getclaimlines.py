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


def mongodb_connection(type="miguri", details={}):
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


def newsql_query(sql, conn):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        row_count = cur.rowcount
        print(f"{row_count} records")
        rows = cur.fetchall()
        result = {"num_records": row_count, "data": rows}
        return result
    except psycopg2.DatabaseError as err:
        print(f"{sql} - {err}")
    cur.close()


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


def get_claims_sql(conn, query, patient_id, r, redis_bool):
    start = datetime.datetime.now()
    key = query + ":" + patient_id
    SQL = ""
    query_result = None
    if redis_bool is True:
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
                    str(ARGS["patient_id"]), str(ARGS["patient_id"])
                )
        logging.debug(f"cache miss -> {patient_id}")
        logging.debug(f"fetching from psql {SQL}")
        query_result = newsql_query(SQL, conn)
        if redis_bool is True:
            load_claims_redis(r, key, query_result)
        num_results = query_result["num_records"]
        logging.debug(f"found {num_results} records")

        print(f"Claims: {num_results}")
        for row in query_result["data"]:
            print(row)
        logging.debug(f"records fetched from psql")

    elapsed = datetime.datetime.now() - start
    logging.debug(f"query took: {elapsed.microseconds / 1000} ms")


def get_claims_mongodb(client, query, patient_id):
    claim = client["claim"]
    result = ''
    start = datetime.datetime.now()
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
    elapsed = datetime.datetime.now() - start
    for data in result:
        print(data)
    # print(result)
    logging.debug(f"query took: {elapsed.microseconds / 1000} ms")


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
                elapsed = datetime.datetime.now() - start
                logging.debug(
                    f"Transaction took: {elapsed.microseconds / 1000} ms")
                logging.debug(f"Transaction completed")
                elapsed_transactions += elapsed.microseconds
    logging.debug(
        f"Transaction average time took for {num_payment} transactions: {elapsed_transactions / (num_payment * 1000)} ms"
    )
    logging.debug(f"Test completed")


# add mongodb query performance


def transaction_postgres(conn, num_payment):
    payment = generate_payments(num_payment)
    cur = conn.cursor()

    SQL_RANDOM = "select claim_id from claim order by random() limit {};".format(
        num_payment
    )
    claim_ids = newsql_query(SQL_RANDOM, conn)
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
        member_id = newsql_query(
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
        logging.debug(f"Transaction took: {elapsed.microseconds / 1000} ms")
        logging.debug(f"Transaction completed")
        elapsed_transactions += elapsed.microseconds
    logging.debug(
        f"Transaction average time took for {num_payment} transactions: {elapsed_transactions / (num_payment * 1000)} ms"
    )
    logging.debug(f"Test completed")


if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    id_map = defaultdict(int)
    conn = pg_connection()
    client = mongodb_connection()
    mongodb = client["healthcare"]
    r = redis_connection()
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "get_claims_sql":
        if "patient_id" not in ARGS:
            print("Send patient_id= argument e.g: python3 gcp_getclaimlines.py action=get_claims_sql patient_id='M-2030000' query=claim or claimLinePayments or  claimMemberProvider ")
            sys.exit(1)
        if ARGS["query"] not in ["claim", "claimLinePayments", "claimMemberProvider"]:
            print("Send patient_id= argument e.g: python3 gcp_getclaimlines.py action=get_claims_sql patient_id='M-2030000' query=claim or claimLinePayments or  claimMemberProvider ")
            sys.exit(1)
        else:
            if ARGS["cache"] == 'False':
                get_claims_sql(conn, ARGS["query"],
                               ARGS["patient_id"], r, False)
            else:
                get_claims_sql(conn, ARGS["query"],
                               ARGS["patient_id"], r, True)
    elif ARGS["action"] == "get_claims_mongodb":
        get_claims_mongodb(mongodb, ARGS["query"], ARGS["patient_id"])
    elif ARGS["action"] == "transaction_mongodb":
        if ARGS["mcommit"] == 'True':
            transaction_mongodb(client, int(ARGS["num_transactions"]), True)
        else:
            transaction_mongodb(client, int(ARGS["num_transactions"]), False)
    elif ARGS["action"] == "transaction_postgres":
        transaction_postgres(conn, int(ARGS["num_transactions"]))
    else:
        print(f'{ARGS["action"]} not found')

    conn.close()
    r.close()
