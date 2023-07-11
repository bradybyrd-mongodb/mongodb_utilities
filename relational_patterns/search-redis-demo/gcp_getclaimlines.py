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
import redis

settings_file = "../relations_settings.json"


def pg_connection(type = "postgres", sdb = 'none'):
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

def redis_connection(type = "redis_local"):
    rhost = settings[type]["host"]
    rusername = settings[type]["username"]
    rpwd = settings[type]["password"]
    r = redis.Redis(host='localhost', port=6379, db=0)
    return r

# SELECT * FROM claim INNER JOIN claim_claimline ON claim.claim_id = claim_claimline.claim_id; 

def get_claims_redis (r , claim_id):
    try:
        r_data = my_dict = json.loads(r.get(claim_id))
        return r_data
    except Exception as error:
        print(f' {error}')

def load_claims_redis (r , claim_id, data):
    try:
        load_state = r.set(claim_id, json.dumps(data, default=dumps))
        print(load_state)
        if load_state is True:
            print("Data loaded to redis")
        return load_state
    except Exception as error:
        print(f' {error}')

def get_claims_sql(conn , claim_id, r):
    query_result = get_claims_redis(r,claim_id)
    if query_result is not None: 
        print('data is in cache')
        print(query_result)
    else:
        sql = "SELECT * FROM claim JOIN claim_claimline ON claim.claim_id = claim_claimline.claim_id where claim.claim_id='{}'".format(str(claim_id))
        print("Data is not in cache, running query against postgres")
        print(sql)
        query_result = newsql_query(sql,conn)
        load_claims_redis(r, claim_id, query_result)     
        num_results = query_result["num_records"]
        print(f'Claims: {num_results}')
        ids = []
        for row in query_result["data"]:
            print(row)
        conn.close()

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
    
# def get_claimlines_sql(conn,claim_ids):
    
#     #payment_fields = sql_helper.column_names("claim_claimline_payment", conn)

#     payment_fields = column_names("claim_claimline_payment", conn)
#     pfields = ', p.'.join(payment_fields)
#     ids_list = '\',\''.join(claim_ids)
#     sql = f'select c.*, p.{pfields} from claim_claimline c '
#     sql += 'INNER JOIN claim_claimline_payment p on c.claim_claimline_id = p.claim_claimline_id '
#     sql += f'WHERE c.claim_id IN (\'{ids_list}\') '
#     sql += "order by c.claim_id"
#     #query_result = sql_helper.sql_query(sql, conn)
#     query_result = newsql_query(sql,conn)
#     num_results = query_result["num_records"]
#     #claimline_fields = sql_helper.column_names("claim_claimline", conn)
#     claimline_fields = column_names("claim_claimline", conn)
#     num_cfields = len(claimline_fields)
#     # Check if the records are found
#     result = {"num_records" : num_results, "data" : []}
#     data = {}
#     if num_results > 0:
#         last_id = "zzzzzz"
#         firsttime = True
#         for row in query_result["data"]:
#             cur_id = row[1]
#             doc = {}
#             if cur_id != last_id:
#                 if not firsttime:
#                     data[last_id] = docs
#                     docs = []
#                 else:
#                     docs = []
#                     firsttime = False
#                 last_id = cur_id
#             #print(row)
#             for k in range(num_cfields):
#                 #print(claimline_fields[k])
#                 doc[claimline_fields[k]] = row[k]
#             sub_doc = {}
#             for k in range(len(payment_fields)):
#                 #print(payment_fields[k])
#                 sub_doc[payment_fields[k]] = row[k + num_cfields]
#             doc["payment"] = sub_doc
#             docs.append(doc)
            
#     result["data"] = data
#     return result

# def column_names(table, conn):
#     sql = f'SELECT column_name FROM information_schema.columns WHERE table_schema = \'public\' AND table_name   = \'{table}\''
#     cur = conn.cursor()
#     #print(sql)
#     try:
#         cur.execute(sql)
#         row_count = cur.rowcount
#         print(f'{row_count} columns')
#     except psycopg2.DatabaseError as err:
#         print(f'{sql} - {err}')
#     rows = cur.fetchall()
#     result = []
#     for i in rows:
#         result.append(i[0])
#     cur.close()
#     return result
    

if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    base_counter = settings["base_counter"]
    id_map = defaultdict(int)
    conn = pg_connection()
    r = redis_connection()

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
    elif ARGS["action"] == "get_claims_sql":
        if "claim_id" not in ARGS:
            print("Send claim_id= argument e.g: python3 gcp_getclaimlines.py action=get_claims_sql claim_id='C-2100000'  ")
        else:
            get_claims_sql(conn, ARGS["claim_id"], r)
        sys.exit(1)
    elif ARGS["action"] == "test_redis":
        redis_connection()
        sys.exit(1)
    # elif ARGS["action"] == "get_claimlines_mdb":
    #     get_claimlines_mdb()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()