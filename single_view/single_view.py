import sys
import os
import csv
#import vcf
from collections import OrderedDict
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
from bbutil import Util
from pymongo import MongoClient
import psycopg2
from faker import Faker

fake = Faker()
letters = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
providers = ["cigna","aetna","anthem","bscbsma","kaiser"]

'''
 #  SingleView/ODS Demo

Remote System:
  Rx
    Table: customers: id, FirstName, LastName, Phone: , Email, Address1Street, Address1Street2, Address1City, Address1State, Address1Zip, Address1Label, Address2Street, Address2Street2, Address2City, Address2State, Address2Zip, Address2Label, Provider, PlanID, Copay
    Table: contacts: CustomerId, ContactDate, Subject, Notes, AuthorId

  Marketing
    Table: people: PeopleId, FirstName, LastName,
    Table: contactMethods: PeopleId, MethodType, Address
    Table: addresses: PeopleId, Street, Street2, City, State, Zip, Label
    Table: communication: PeopleId, ContactDate, Type, Subject, Notes, AuthorId

  Billing
    Table: customers: id, Title, FirstName, LastName, Suffix, Phone: , Email, Street, Street2, City, StateProvince, PostalCode, AddressNote, ModifiedAt, ModifiedBy, LastPurchaseAt, isActive
    Table: relatedParty: customerId, relationship


ODS Model
{
  CustomerID: ,
  FirstName: ,
  LastName: ,
  Phone: ,
  Email: ,
  Version: 1.0,
  Address: [
    {Type: "work", Street: , Line2: , City: , State: , Zip:}
  ]
  Sources : [
    {each}
  ]
}

    python3 single_view.py action=load_mysql

# Startup Env:
    Atlas M10BasicAgain
    PostgreSQL
      export PATH="/usr/local/opt/postgresql@9.6/bin:$PATH"
      pg_ctl -D /usr/local/var/postgresql@9.6 start
      create database single_view with owner bbadmin;
      psql --username bbadmin single_view
'''
settings_file = "single_view_settings.json"

class id_generator:
    def __init__(self, details = {}):
        self.tally = 100000
        if "seed" in details:
            self.tally = seed
    def set(self, seed):
        self.tally = seed
        return(self.tally)

    def get(self):
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
    pgcon = pg_connection()
    tables = execute_ddl(ddl_action, pgcon)
    # Spawn processes
    num_procs = settings["process_count"]
    jobs = []
    inc = 0
    multiprocessing.set_start_method("fork", force=True)
    for item in range(num_procs):
        p = multiprocessing.Process(target=worker_load, args = (item,tables,))
        jobs.append(p)
        p.start()
        time.sleep(1)
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def worker_load(ipos, tables):
    #  Reads EMR sample file and finds values
    bb.message_box("Loading Synth Data", "title")
    pgcon = pg_connection()
    cur_process = multiprocessing.current_process()
    IDGEN = id_generator()
    MASTER_CUSTOMERS = []
    bb.logit('Current process is %s %s' % (cur_process.name, cur_process.pid))
    file_log(f'New process {cur_process.name}')
    start_time = datetime.datetime.now()
    tables = execute_ddl('info')
    worker_customer_load(pgcon,tables)
    worker_claim_load(pgcon,tables)
    worker_rx_load(pgcon,tables)
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    pgcon.close()
    file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
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

def worker_claim_load(conn, tables):
    #  Reads EMR sample file and finds values
    item = 'claim_customers'
    cur_process = multiprocessing.current_process()
    batch_size = settings["batch_size"]
    num_records = settings["num_records"]
    batches = int(num_records / batch_size)
    table_names = list(tables)
    bb.message_box(f"Loading Data for {item}", "title")
    start_time = datetime.datetime.now()
    table = item
    cust_tot = len(MASTER_CUSTOMERS)
    bulk_docs = []
    addr_docs = []
    com_docs = []
    atypes = ["home","work","vacation"]
    cnt = 0
    tot = 0
    for batch in range(batches):
        for inc in range(batch_size):
            #print(record.FORMAT)
            age = random.randint(28,84)
            year = 2020 - age
            month = random.randint(1,12)
            day = random.randint(1,28)
            name = fake.name()
            id = IDGEN.get()
            doc = {}
            doc['member_id'] = id
            doc['first_name'] = name.split(" ")[0]
            doc['last_name'] = name.split(" ")[1]
            doc['birth_date'] = datetime.datetime(year,month,day, 10, 45)
            doc['email'] = f'{name.replace(" ",".")}@randomfirm.com'
            doc["gender"] = random.choice(["M","F"])
            doc['phone'] = fake.phone_number()
            doc['mobile_phone'] = fake.phone_number()
            for k in range(random.randint(0,2)):
                addr = {}
                addr["member_id"] = id
                addr["type"] = atypes[k]
                addr["street"] = fake.street_address()
                addr["address2"] = ""
                addr["city"] = fake.city()
                addr["state"] = fake.state_abbr()
                addr["zip"] = fake.zipcode()
                addr_docs.append(addr)
            if tot < cust_tot:
                name = MASTER_CUSTOMERS[tot]["name"]
                doc['first_name'] = name.split(" ")[0]
                doc['last_name'] = name.split(" ")[1]
                doc['birth_date'] = MASTER_CUSTOMERS[tot]["birth_date"]
            else:
                MASTER_CUSTOMERS.append({"source" : "medical_claims", "name" : name, "id" : id, "birth_date" : doc['birth_date']})
            doc["is_active"] = random.choice(["Y","N"])
            start = datetime.date(year=2015, month=1, day=1)
            end = datetime.date(year=2019, month=1, day=5)
            doc["created_at"] = fake.date_between(start_date=start, end_date=end)
            end = datetime.date(year=2021, month=1, day=5)
            start = datetime.date(year=2019, month=1, day=1)
            doc["modified_at"] = fake.date_between(start_date=start, end_date=end)
            #pprint.pprint(doc)
            bulk_docs.append(doc)
            medical_claims(tables, conn, doc)
            cnt += 1
            tot += 1
        record_loader(tables,table,bulk_docs,conn)
        record_loader(tables,"claim_address",addr_docs,conn)
        bulk_docs = []
        addr_docs = []
        cnt = 0
        bb.logit(f"{cur_process.name} Inserting Bulk, Total:{tot}")
        file_log(f"{cur_process.name} Inserting Bulk, Total:{tot}")
        #oktogo = checkfile()
        #if not oktogo:
        #    bb.logit("Received stop signal")
        #    break
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def worker_rx_load(conn, tables):
    item = 'rx_customers'
    cur_process = multiprocessing.current_process()
    batch_size = settings["batch_size"]
    num_records = settings["num_records"]
    batches = int(num_records / batch_size)
    cust_tot = len(MASTER_CUSTOMERS)
    table_names = list(tables)
    bb.message_box(f"Loading Data for {item}", "title")
    start_time = datetime.datetime.now()
    table = item
    bulk_docs = []
    cnt = 0
    tot = 0
    for batch in range(batches):
        for inc in range(batch_size):
            #print(record.FORMAT)
            age = random.randint(28,84)
            year = 2020 - age
            month = random.randint(1,12)
            day = random.randint(1,28)
            name = fake.name()
            id = IDGEN.get()
            doc = {}
            doc['rxmem_id'] = id
            doc['title'] = random.choice(["Mr","Ms","Mrs","Dr","HRM"])
            doc['first_name'] = name.split(" ")[0]
            doc['last_name'] = name.split(" ")[1]
            doc["suffix"] = random.choice(["","Jr","","III","","Sr"])
            doc['birth_date'] = datetime.datetime(year,month,day, 10, 45)
            doc['phone'] = fake.phone_number()
            doc['email'] = f'{name.replace(" ",".")}@randomfirm.com'
            doc["gender"] = random.choice(["M","F"])
            doc["address_street"] = fake.street_address()
            doc["address_line2"] = ""
            doc["address_city"] = fake.city()
            doc["address_stateprov"] = fake.state_abbr()
            doc["address_postalcode"] = fake.zipcode()
            if tot < cust_tot:
                name = MASTER_CUSTOMERS[tot]["name"]
                doc['first_name'] = name.split(" ")[0]
                doc['last_name'] = name.split(" ")[1]
                doc['birth_date'] = MASTER_CUSTOMERS[tot]["birth_date"]
            else:
                MASTER_CUSTOMERS.append({"source" : "billingDB", "name" : name, "id" : id})
            start = datetime.date(year=2015, month=1, day=1)
            end = datetime.date(year=2019, month=1, day=5)
            doc["created_at"] = fake.date_between(start_date=start, end_date=end)
            end = datetime.date(year=2021, month=1, day=5)
            start = datetime.date(year=2019, month=1, day=1)
            doc["modified_by_id"] = random.randint(100000,200000)
            doc["modified_at"] = fake.date_between(start_date=start, end_date=end)
            #pprint.pprint(doc)
            bulk_docs.append(doc)
            rx_claims(tables, conn, doc)
            cnt += 1
            tot += 1
        record_loader(tables,table,bulk_docs,conn)
        bulk_docs = []
        cnt = 0
        bb.logit(f"{cur_process.name} Inserting Bulk, Total:{tot}")
        file_log(f"{cur_process.name} Inserting Bulk, Total:{tot}")
        #oktogo = checkfile()
        #if not oktogo:
        #    bb.logit("Received stop signal")
        #    break
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    file_log(f"{cur_process.name} - Bulk Load took {execution_time} seconds")
    bb.logit(f"{cur_process.name} - Bulk Load took {execution_time} seconds")

def medical_claims(tables, conn, curdoc):
    item = 'medical_claim'
    cur_process = multiprocessing.current_process()
    batch_size = settings["batch_size"]
    num_records = settings["num_records"]
    batches = int(num_records / batch_size)
    start_time = datetime.datetime.now()
    table = item
    bulk_docs = []
    cnt = 0
    tot = 0
    for inc in range(random.randint(1,10)):
        doc = {}
        doc['member_id'] = curdoc["member_id"]
        doc['item'] = fake.bs()
        doc['message'] = fake.paragraph(4)
        start = datetime.date(year=2015, month=1, day=1)
        end = datetime.date(year=2019, month=1, day=5)
        doc['amount'] = random.randint(1000,27000)
        doc["created_at"] = fake.date_between(start_date=start, end_date=end)
        doc['paid_status'] = random.choice(["billed", "paid", "late"])
        doc["provider"] = random.choice(providers)
        #pprint.pprint(doc)
        bulk_docs.append(doc)
        cnt += 1
        tot += 1
    record_loader(tables,table,bulk_docs,conn)
    bb.logit(f"{cur_process.name} Inserting SubBulk, Total:{tot}")
    file_log(f"{cur_process.name} Inserting SubBulk, Total:{tot}")

def rx_claims(tables, conn, curdoc):
    item = 'rx_claim'
    cur_process = multiprocessing.current_process()
    batch_size = settings["batch_size"]
    num_records = settings["num_records"]
    batches = int(num_records / batch_size)
    start_time = datetime.datetime.now()
    table = item
    bulk_docs = []
    cnt = 0
    tot = 0
    for inc in range(random.randint(1,10)):
        doc = {}
        doc['member_id'] = curdoc['rxmem_id']
        doc['item'] = fake.bs()
        doc['message'] = fake.paragraph(4)
        start = datetime.date(year=2015, month=1, day=1)
        end = datetime.date(year=2019, month=1, day=5)
        doc['amount'] = random.uniform(10.01,270.01)
        doc['covered_amount'] = doc["amount"] * random.choice([0.2,0.4,0.6,0.8])
        doc["created_at"] = fake.date_between(start_date=start, end_date=end)
        doc['paid_status'] = random.choice(["billed", "paid", "late"])
        doc["provider"] = random.choice(providers)
        #pprint.pprint(doc)
        bulk_docs.append(doc)
        cnt += 1
        tot += 1
    record_loader(tables,table,bulk_docs,conn)
    bb.logit(f"{cur_process.name} Inserting SubBulk, Total:{tot}")
    file_log(f"{cur_process.name} Inserting SubBulk, Total:{tot}")


def single_record_loader(tables, table, recs, conn):
    # insert_into table fields () values ();
    cur = conn.cursor()
    fields = list(recs[0])
    prefix = f"use {tables[table]['database']};"
    sql = f"{tables[table]['insert']}"
    vals = []
    for record in recs:
        stg = list()
        for k in record:
            stg.append(record[k])
        print(sql)
        print(stg)
        try:
            cur.execute(sql, tuple(stg))
            conn.commit()
            print(f'{cur.rowcount} inserted')
        except psycopg2.DatabaseError as err:
            bb.logit(f'{table} - {err}')
    cur.close()

def record_loader(tables, table, recs, nconn):
    # insert_into table fields () values ();
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
    conn.close()
#----------------------------------------------------------#
#----------------------------------------------------------#
# SQL DDL Routines
def execute_ddl(action = "info", pgconn = 'none'):
    if "ddl" in ARGS:
        action = ARGS["ddl"]
    bb.logit(f"DDL Info - {action}")
    tables = {}
    database = settings["postgres"]["database"]
    table = 'provider_info'
    tables[table] = {
    "ddl" : (
        f"CREATE TABLE {table} ("
        "  id SERIAL PRIMARY KEY,"
        "  member_id varchar(20) NOT NULL,"
        "  benefit_plan_id varchar(30) NOT NULL,"
        "  birth_date date,"
        "  first_name varchar(30) NOT NULL,"
        "  last_name varchar(30) NOT NULL,"
        "  phone varchar(40) NOT NULL,"
        "  email varchar(50) NOT NULL,"
        "  gender varchar(2) NOT NULL,"
        "  address1_type varchar(20) NOT NULL,"
        "  address1_street varchar(100) NOT NULL,"
        "  address1_line2 varchar(40),"
        "  address1_city varchar(40) NOT NULL,"
        "  address1_state varchar(5) NOT NULL,"
        "  address1_zipcode varchar(5) NOT NULL,"
        "  address2_type varchar(20) NOT NULL,"
        "  address2_street varchar(100) NOT NULL,"
        "  address2_line2 varchar(40),"
        "  address2_city varchar(40) NOT NULL,"
        "  address2_state varchar(10) NOT NULL,"
        "  address2_zipcode varchar(5) NOT NULL"
        ")"
    ),
    "database" : database,
    "fields" : ['member_id', 'benefit_plan_id', 'birth_date', 'first_name', 'last_name','phone','email','gender','address1_type','address1_street','address1_line2',
    'address1_city','address1_state','address1_zipcode','address2_type','address2_street','address2_line2','address2_city','address2_state','address2_zipcode']
    }
    fmts = value_codes(tables[table]["fields"])
    tables[table]["insert"] = f'insert into {table} ({",".join(tables[table]["fields"])}) values ({fmts});'
    # ClaimCUstomers
    table = 'claim_customers'
    tables[table] = {
    "ddl" : (
        f"CREATE TABLE {table} ("
        "  id SERIAL PRIMARY KEY,"
        "  member_id varchar(20) NOT NULL,"
        "  first_name varchar(30) NOT NULL,"
        "  last_name varchar(30) NOT NULL,"
        "  birth_date timestamp,"
        "  email varchar(50) NOT NULL,"
        "  gender varchar(2) NOT NULL,"
        "  phone varchar(40) NOT NULL,"
        "  mobile_phone varchar(40),"
        "  is_active varchar(2),"
        "  created_at timestamp NOT NULL,"
        "  modified_at timestamp"
        ")"
        ),
    "database" : database,
    "fields" : ['member_id', 'first_name', 'last_name','birth_date','email','gender','phone','mobile_phone','is_active',
    'created_at','modified_at']
     }
    fmts = value_codes(tables[table]["fields"])
    tables[table]["insert"] = f'insert into {table} ({",".join(tables[table]["fields"])}) values ({fmts});'
    # Rx_customers
    table = 'rx_customers'
    tables[table] = {
    "ddl" : (
        f"CREATE TABLE {table} ("
        "  id SERIAL PRIMARY KEY,"
        "  rxmem_id varchar(20) NOT NULL,"
        "  title varchar(30),"
        "  first_name varchar(30) NOT NULL,"
        "  last_name varchar(30) NOT NULL,"
        "  suffix varchar(30),"
        "  birth_date timestamp,"
        "  phone varchar(40),"
        "  email varchar(50) NOT NULL,"
        "  gender varchar(2) NOT NULL,"
        "  address_street varchar(100) NOT NULL,"
        "  address_line2 varchar(40),"
        "  address_city varchar(40) NOT NULL,"
        "  address_stateprov varchar(5) NOT NULL,"
        "  address_postalcode varchar(10) NOT NULL,"
        "  created_at timestamp NOT NULL,"
        "  modified_by_id integer,"
        "  modified_at timestamp"
        ")"
    ),
    "database" : database,
    "fields" : ['rxmem_id', 'title', 'first_name', 'last_name','suffix','birth_date','phone','email','gender','address_street','address_line2',
    'address_city','address_stateprov','address_postalcode','created_at','modified_by_id','modified_at']
    }
    fmts = value_codes(tables[table]["fields"])
    tables[table]["insert"] = f'insert into {table} ({",".join(tables[table]["fields"])}) values ({fmts});'

    # ClaimAddress
    table = 'claim_address'
    tables[table] = {
    "ddl" : (
        f"CREATE TABLE {table} ("
        "  id SERIAL PRIMARY KEY,"
        "  member_id varchar(20) NOT NULL,"
        "  type varchar(20),"
        "  street varchar(100) NOT NULL,"
        "  address2 varchar(100),"
        "  city varchar(40) NOT NULL,"
        "  state varchar(10) NOT NULL,"
        "  zipcode varchar(10) NOT NULL"
        ")"),
    "database" : database,
    "fields" : ['member_id', 'type', 'street','address2','city','state','zipcode']
     }
    fmts = value_codes(tables[table]["fields"])
    tables[table]["insert"] = f'insert into {table} ({",".join(tables[table]["fields"])}) values ({fmts});'
    # medical claim
    table = 'medical_claim'
    tables[table] = {
    "ddl" : (
        f"CREATE TABLE {table} ("
        "  id SERIAL PRIMARY KEY,"
        "  member_id varchar(20) NOT NULL,"
        "  item varchar(50),"
        "  message text,"
        "  amount real,"
        "  created_at timestamp,"
        "  paid_status varchar(30),"
        "  provider varchar(30)"
        ")"),
    "database" : database,
    "fields" : ['member_id', 'item','message','amount','created_at','paid_status','provider']
    }
    fmts = value_codes(tables[table]["fields"])
    tables[table]["insert"] = f'insert into {table} ({",".join(tables[table]["fields"])}) values ({fmts});'
    # rx claim
    table = 'rx_claim'
    tables[table] = {
    "ddl" : (
        f"CREATE TABLE {table} ("
        "  id SERIAL PRIMARY KEY,"
        "  member_id varchar(20) NOT NULL,"
        "  item varchar(50),"
        "  message text,"
        "  amount real,"
        "  covered_amount real,"
        "  created_at timestamp,"
        "  paid_status varchar(30),"
        "  provider varchar(30)"
        ")"),
    "database" : database,
    "fields" : ['member_id', 'item','message','amount','covered_amount','created_at','paid_status','provider']
    }
    fmts = value_codes(tables[table]["fields"])
    tables[table]["insert"] = f'insert into {table} ({",".join(tables[table]["fields"])}) values ({fmts});'

    if action == "create":
        cursor = pgconn.cursor()
        for table_name in tables:
            table_description = tables[table_name]['ddl']
            #table_description = f"{tables[table_name]['ddl']}"
            try:
                bb.logit(f"Creating table {table_name}")
                print(table_description)
                cursor.execute(table_description)
            except psycopg2.DatabaseError as err:
                bb.logit(err)
            else:
                print("OK")
        pgconn.commit()
        cursor.close()
    elif action == "drop":
        cursor = pgconn.cursor()
        for table_name in tables:
            bb.logit(f"Dropping Table {table_name}")
            prefix = f'use {tables[table]["database"]}\n'
            try:
                cursor.execute(f'DROP TABLE {table_name};')
            except psycopg2.DatabaseError as err:
                bb.logit(err)
        pgconn.commit()
        cursor.close()
    return(tables)

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

#----------------------------------------------------------#
#----------------------------------------------------------#
# Mongo Document Routines for MasterData
#  Queries API Style

def get_providers():
    """ query data from the providers table """
#    try:
    conn = pg_connection()
    mdb = client_connection()
    cur = conn.cursor()
    database = settings["database"]
    coll = "party"
    table = "provider_info"
    sql = f"select * from {table}"
    tables = execute_ddl('info')
    cur.execute(sql)
    bb.message_box(f'Providers: {cur.rowcount}')
    row = cur.fetchone()
    fields = tables[table]['fields']
    bulk_docs = []
    tot = 0
    while row is not None:
        #print(row)
        doc = {}
        orig_doc = {}
        cnt = 0
        source_id = 0
        addr1 = {}
        addr2 = {}
        for fld in row:
            if fld.__class__.__name__ == 'date':
                fld = datetime.datetime.combine(fld, datetime.datetime.min.time())
            if cnt == 0:
                #bb.logit(f'ID: {fld}')
                orig_doc["source"] = table
                orig_doc["source_id"] = fld
                orig_doc["import_date"] = datetime.datetime.now()
                source_id = fld
            else:
                name = fields[cnt - 1]
                orig_doc[name] = fld
                #bb.logit(f'{name}: {fld}')
                if "address1" in name:
                    addr1[name.replace("address1_","")] = fld
                elif "address2" in name:
                    addr2[name.replace("address2_","")] = fld
                else:
                    doc[fields[cnt - 1]] = fld
            cnt += 1
        doc["address"] = [addr1,addr2]
        doc["medical_claims"] = []
        doc["rx_claims"] = []
        doc["sources"] = {}
        doc["last_update_source"] = table
        doc["last_modified_at"] = datetime.datetime.now()
        doc["change_history"] = []
        doc["sources"][table] = orig_doc
        doc["change_history"].append({"last_modified_at": datetime.datetime.now(), "source" : table, "change" : "New document"})
        doc["version"] = "1.0"
        bulk_docs.append(doc)
        tot += 1
        bb.logit(f'Importing {tot}')
        row = cur.fetchone()
    cur.close()
    #print(bulk_docs)
    mdb[database][coll].insert_many(bulk_docs)
#    except (Exception, psycopg2.DatabaseError) as error:
#        print(error)
#    finally:
#        if conn is not None:
    conn.close()

def get_claims():
    """ query data from the claim_customers table """
#    try:
    conn = pg_connection()
    mdb = client_connection()
    cur = conn.cursor()
    cur_addr = conn.cursor()
    database = settings["database"]
    coll = "party"
    table = "claim_customers"
    child_table = "claim_address"
    sql = f"select * from {table}"
    tables = execute_ddl('info')
    cur.execute(sql)
    bb.message_box(f'Claim_Customers: {cur.rowcount}')
    row = cur.fetchone()
    fields = tables[table]['fields']
    bulk_docs = []
    tot = 0
    while row is not None:
        #print(row)
        doc = {}
        orig_doc = {}
        cnt = 0
        source_id = 0
        addr1 = {}
        addr2 = {}
        for fld in row:
            if fld.__class__.__name__ == 'date':
                fld = datetime.datetime.combine(fld, datetime.datetime.min.time())
            if cnt == 0:
                #bb.logit(f'ID: {fld}')
                orig_doc["source"] = table
                orig_doc["source_id"] = fld
                orig_doc["import_date"] = datetime.datetime.now()
                source_id = fld
            else:
                name = fields[cnt - 1]
                orig_doc[name] = fld
                #bb.logit(f'{name}: {fld}')
                doc[fields[cnt - 1]] = fld
            cnt += 1
        addrs = get_customer_address(orig_doc["member_id"], tables[child_table], conn)
        orig_doc["recent_claims"] = []
        doc["address"] = addrs
        orig_doc["address"] = addrs
        doc["last_update_source"] = table
        doc["last_modified_at"] = datetime.datetime.now()
        doc["sources"] = {}
        doc["change_history"] = []
        doc["sources"][table] = orig_doc
        doc["change_history"].append({"last_modified_at": datetime.datetime.now(), "source" : table, "change" : "Added new source"})
        doc["version"] = "1.0"
        ans = mdb[database][coll].update_one({"last_name" : doc["last_name"],"first_name": doc["first_name"]}, {
            "$set" : {f'sources.{table}' : orig_doc},
            "$addToSet" : {"change_history" : {"last_modified_at": datetime.datetime.now(), "source" : table, "change" : "Added new source"}}
        })
        if ans is None:
            mdb[database][coll].insert_one(doc)

        #bulk_docs.append(doc)
        tot += 1
        bb.logit(f'Importing {tot}')
        row = cur.fetchone()
    cur.close()
    conn.close()
    mdb.close()
#    except (Exception, psycopg2.DatabaseError) as error:
#        print(error)
#    finally:
#        if conn is not None:

def get_customer_address(member_id, table_info, conn):
    table = "claim_address"
    cur = conn.cursor()
    sql = f"select * from {table} where member_id = '{member_id}'"
    cur.execute(sql)
    row = cur.fetchone()
    fields = table_info['fields']
    bulk_docs = []
    tot = 0
    while row is not None:
        doc = {}
        cnt = 0
        source_id = 0
        for fld in row:
            if fld.__class__.__name__ == 'date':
                fld = datetime.datetime.combine(fld, datetime.datetime.min.time())
            if cnt == 0:
                #bb.logit(f'ID: {fld}')
                doc["source"] = table
                doc["source_id"] = fld
                doc["import_date"] = datetime.datetime.now()
                source_id = fld
            else:
                name = fields[cnt - 1]
                doc[name] = fld
            cnt += 1
        bb.logit(f'Found {member_id} - {doc["city"]}')
        bulk_docs.append(doc)
        row = cur.fetchone()
    cur.close()
    return bulk_docs

def get_rx_customers():
    """ query data from the rx_customers table """
#    try:
    conn = pg_connection()
    mdb = client_connection()
    cur = conn.cursor()
    database = settings["database"]
    coll = "party"
    table = "rx_customers"
    sql = f"select * from {table}"
    tables = execute_ddl('info')
    cur.execute(sql)
    bb.message_box(f'Rx_Customers: {cur.rowcount}')
    row = cur.fetchone()
    fields = tables[table]['fields']
    bulk_docs = []
    tot = 0
    while row is not None:
        #print(row)
        doc = {}
        orig_doc = {}
        cnt = 0
        source_id = 0
        addr1 = {}
        addr2 = {}
        for fld in row:
            if fld.__class__.__name__ == 'date':
                fld = datetime.datetime.combine(fld, datetime.datetime.min.time())
            if cnt == 0:
                #bb.logit(f'ID: {fld}')
                orig_doc["source"] = table
                orig_doc["source_id"] = fld
                orig_doc["import_date"] = datetime.datetime.now()
                source_id = fld
            else:
                name = fields[cnt - 1]
                orig_doc[name] = fld
                #bb.logit(f'{name}: {fld}')
                if "address1" in name:
                    addr1[name.replace("address1_","")] = fld
                elif "address2" in name:
                    addr2[name.replace("address2_","")] = fld
                else:
                    doc[fields[cnt - 1]] = fld
            cnt += 1
        #doc["address"] = [addr1,addr2]
        orig_doc["recent_claims"] = []
        doc["last_update_source"] = table
        doc["last_modified_at"] = datetime.datetime.now()
        doc["sources"] = {}
        doc["change_history"] = []
        doc["change_history"].append({"last_modified_at": datetime.datetime.now(), "source" : table, "change" : "Added new source"})
        doc["sources"][table] = orig_doc
        doc["version"] = "1.0"
        ans = mdb[database][coll].update_one({"last_name" : doc["last_name"],"first_name": doc["first_name"]}, {
            "$set" : {f'sources.{table}' : orig_doc},
            "$addToSet" : {"change_history" : {"last_modified_at": datetime.datetime.now(), "source" : table, "change" : "Added new source"}}
        })
        if ans is None:
            mdb[database][coll].insert_one(doc)

        #bulk_docs.append(doc)
        tot += 1
        bb.logit(f'Importing {tot}')
        row = cur.fetchone()
    cur.close()
    conn.close()
    mdb.close()
#    except (Exception, psycopg2.DatabaseError) as error:
#        print(error)
#    finally:
#        if conn is not None:

def append_claims():
    """ query data from the medical_claim table """
#    try:
    conn = pg_connection()
    mdb = client_connection()
    cur = conn.cursor()
    database = settings["database"]
    coll = "claim"
    parent_coll = "party"
    bulk_size = 100
    table = "medical_claim"
    top_size = 10
    parent_table = "claim_customers"
    sql = f"select * from {table} order by member_id, created_at DESC"
    tables = execute_ddl('info')
    cur.execute(sql)
    bb.message_box(f'MedicalClaim: {cur.rowcount}')
    row = cur.fetchone()
    fields = tables[table]['fields']
    bulk_docs = []
    bulk_orig = []
    last_mem_id = "99999xxx"
    tot = 0
    while row is not None:
        #print(row)
        doc = {}
        orig_doc = {}
        cnt = 0
        source_id = 0
        for fld in row:
            if fld.__class__.__name__ == 'date':
                fld = datetime.datetime.combine(fld, datetime.datetime.min.time())
            if cnt == 0:
                #bb.logit(f'ID: {fld}')
                source_id = fld
                orig_doc["source"] = table
                orig_doc["source_id"] = source_id
                orig_doc["import_date"] = datetime.datetime.now()
                doc["source_id"] = source_id
            else:
                name = fields[cnt - 1]
                orig_doc[name] = fld
                if name == "member_id" and last_mem_id == "none":
                    last_mem_id = fld
                #bb.logit(f'{name}: {fld}')
                doc[fields[cnt - 1]] = fld
            cnt += 1
        bulk_orig.append(orig_doc)
        if len(bulk_orig) > bulk_size:
            ans = mdb[database][coll].insert_many(bulk_orig)
            bulk_orig = []
        if last_mem_id != orig_doc["member_id"]:
            ans = mdb[database][parent_coll].update_one({f'sources.{parent_table}.member_id' : last_mem_id}, {
                "$set" : {f'sources.{parent_table}.recent_claims' : bulk_docs},
                "$addToSet" : {"medical_claims" : {"$each" : bulk_docs}}
            })
            bb.logit(f'Bulk: {len(bulk_docs)}')
            bulk_docs = []
            bulk_docs.append(doc)
            last_mem_id = orig_doc["member_id"]
        else:
            if len(bulk_docs) < top_size:
                bulk_docs.append(doc)
        tot += 1
        bb.logit(f'Importing {tot}')
        row = cur.fetchone()

    if len(bulk_docs) > 0:
        ans = mdb[database][parent_coll].update_one({f'sources.{parent_table}.member_id' : last_mem_id}, {
            "$set" : {f'sources.{parent_table}.recent_claims' : bulk_docs},
            "$addToSet" : {"medical_claims" : {"$each" : bulk_docs}}
        })
        if len(bulk_orig) > 0:
            ans = mdb[database][coll].insert_many(bulk_orig)
    cur.close()
    conn.close()
    mdb.close()
#    except (Exception, psycopg2.DatabaseError) as error:
#        print(error)
#    finally:
#        if conn is not None:

def append_rx_claims():
    """ query data from the medical_claim table """
#    try:
    conn = pg_connection()
    mdb = client_connection()
    cur = conn.cursor()
    database = settings["database"]
    coll = "claim"
    parent_coll = "party"
    bulk_size = 100
    table = "rx_claim"
    top_size = 10
    parent_table = "rx_customers"
    sql = f"select * from {table} order by member_id, created_at DESC"
    tables = execute_ddl('info')
    cur.execute(sql)
    bb.message_box(f'RxClaim: {cur.rowcount}')
    row = cur.fetchone()
    fields = tables[table]['fields']
    bulk_docs = []
    bulk_orig = []
    last_mem_id = "99999xxx"
    tot = 0
    while row is not None:
        #print(row)
        doc = {}
        orig_doc = {}
        cnt = 0
        source_id = 0
        for fld in row:
            if fld.__class__.__name__ == 'date':
                fld = datetime.datetime.combine(fld, datetime.datetime.min.time())
            if cnt == 0:
                #bb.logit(f'ID: {fld}')
                source_id = fld
                orig_doc["source"] = table
                orig_doc["source_id"] = source_id
                orig_doc["import_date"] = datetime.datetime.now()
                doc["source_id"] = source_id
            else:
                name = fields[cnt - 1]
                orig_doc[name] = fld
                if name == "member_id" and last_mem_id == "none":
                    last_mem_id = fld
                #bb.logit(f'{name}: {fld}')
                doc[fields[cnt - 1]] = fld
            cnt += 1
        bulk_orig.append(orig_doc)
        if len(bulk_orig) > bulk_size:
            ans = mdb[database][coll].insert_many(bulk_orig)
            bulk_orig = []
        if last_mem_id != orig_doc["member_id"]:
            ans = mdb[database][parent_coll].update_one({f'sources.{parent_table}.rxmem_id' : last_mem_id}, {
                "$set" : {f'sources.{parent_table}.recent_claims' : bulk_docs},
                "$addToSet" : {"medical_claims" : {"$each" : bulk_docs}}
            })
            bulk_docs = []
            bulk_docs.append(doc)
            last_mem_id = orig_doc["member_id"]
        else:
            if len(bulk_docs) < top_size:
                bulk_docs.append(doc)
        tot += 1
        bb.logit(f'Importing {tot}')
        row = cur.fetchone()
    if len(bulk_docs) > 0:
        ans = mdb[database][parent_coll].update_one({f'sources.{parent_table}.rxmem_id' : last_mem_id}, {
            "$set" : {f'sources.{parent_table}.recent_claims' : bulk_docs},
            "$addToSet" : {"medical_claims" : {"$each" : bulk_docs}}
        })
        if len(bulk_orig) > 0:
            ans = mdb[database][coll].insert_many(bulk_orig)
        #bulk_docs.append(doc)
        tot += 1
        bb.logit(f'Importing {tot}')
        row = cur.fetchone()
    cur.close()
    conn.close()
    mdb.close()
#    except (Exception, psycopg2.DatabaseError) as error:
#        print(error)
#    finally:
#        if conn is not None:

def append_claims_addresses():
    """ Add address information for the claim_customers """
#    try:
    conn = pg_connection()
    mdb = client_connection()
    cur = conn.cursor()
    database = settings["database"]
    parent_coll = "party"
    coll = "claim"
    table = "claim_address"
    parent_table = "claim_customers"
    sql = f"select * from {table} order by member_id"
    tables = execute_ddl('info')
    cur.execute(sql)
    bb.message_box(f'MedicalClaim - addressesv: {cur.rowcount}')
    row = cur.fetchone()
    fields = tables[table]['fields']
    bulk_docs = []
    last_mem_id = "none"
    tot = 0
    while row is not None:
        #print(row)
        doc = {}
        orig_doc = {}
        cnt = 0
        source_id = 0
        bulk_docs = []
        for fld in row:
            if fld.__class__.__name__ == 'date':
                fld = datetime.datetime.combine(fld, datetime.datetime.min.time())
            if cnt == 0:
                #bb.logit(f'ID: {fld}')
                orig_doc["source"] = table
                orig_doc["source_id"] = fld
                orig_doc["import_date"] = datetime.datetime.now()
                source_id = fld
            else:
                name = fields[cnt - 1]
                if name == "member_id" and last_mem_id == "none":
                    last_mem_id = fld
                orig_doc[name] = fld
                #bb.logit(f'{name}: {fld}')
                doc[fields[cnt - 1]] = fld
            cnt += 1
        if last_mem_id != orig_doc["member_id"]:
            ans = mdb[database]["party"].update_many({f'sources.{parent_table}.member_id' : last_mem_id}, {"$set" : {"address" : bulk_docs}})
            bulk_docs = []
            last_mem_id = orig_doc["member_id"]
        else:
            bulk_docs.append(doc)
        tot += 1
        bb.logit(f'Importing {tot}')
        row = cur.fetchone()
    if len(bulk_docs) > 0:
        ans = mdb[database]["party"].update_many({f'sources.{parent_table}.member_id' : last_mem_id}, {"$set" : {"address" : bulk_docs}})
    cur.close()
    conn.close()
    mdb.close()
#    except (Exception, psycopg2.DatabaseError) as error:
#        print(error)
#    finally:
#        if conn is not None:

# Sync from MSQL to Mongo
def feed_manager():
    feed_file = "kafka_feeds.json"
    feeds = bb.read_json(feed_file)
    mdb = client_connection()
    inc = 0
    while True:
        bb.message_box(f"Feed Manager (iter: {inc})")
        for db in feeds:
            result = sync_feed(mdb, db, feeds[db])
            for k in result:
                feeds[db]["tables"][k] = result[k]
            bb.save_json(feed_file,feeds)
        inc += 1
        time.sleep(2)


def sync_feed(mdbconn, my_db, details):
    db = mdbconn["customer360"]
    conn = mysql_connection('mysql', my_db)
    bb.logit(f"Sync: {my_db}")
    id_field = details["id_field"]
    tables = details["tables"]
    cursor = conn.cursor()
    result = {}
    for table in tables:
        coll_name = f'feed_{table}'
        last_id = tables[table]
        result[table] = last_id
        sql = f'select * from {table} where id > {last_id} order by {id_field}'
        bb.logit(f" - Query: {sql}")
        cursor.execute(sql)
        cnt = 0
        tot = 0
        col_names = [i[0] for i in cursor.description]
        #print(col_names)
        #bulk_docs = []
        for row in cursor:
            #bulk_docs = []
            #print(row)
            doc = OrderedDict()
            for item in row:
                field = col_names[row.index(item)]
                #print(item.__class__.__name__)
                if type(item) is datetime.date:
                    doc[field] = datetime.datetime.combine(item, datetime.datetime.min.time())
                elif type(item) is Decimal:
                    doc[field] = bson.decimal128.Decimal128(str(item))
                else:
                    doc[field] = item
            print(".", end="")
            if tot % 100 == 0:
                print(f"Total: {tot}")
            tot += 1
            doc[f'key_id_{my_db}'] = doc[id_field]
            db[coll_name].insert_one(doc)
            result[table] = doc[id_field]

    cursor.close()
    conn.close()
    return result

def merge_feeds():
    mdb = client_connection()
    db = mdb["customer360"]
    bb.message_box("Merging Feeds")
    batch = "feeds"
    items = cc.batches[batch]
    query_list(items,db)


def check_file(type = "delete"):
    #  file loader.ctl
    ctl_file = "loader.ctl"
    result = True
    with open(ctl_file, 'w', newline='') as controlfile:
        status = controlfile.read()
        if "stop" in status:
            result = False
    return(result)

def file_log(msg):
    if not "file" in ARGS:
        return("goody")
    ctl_file = "run_log.txt"
    cur_date = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    stamp = f"{cur_date}|I> "
    with open(ctl_file, 'a') as lgr:
        lgr.write(f'{stamp}{msg}\n')


#----------------------------------------------------------------------#
#   Utility Routines
#----------------------------------------------------------------------#

def client_connection(type = "uri", details = {}):
    mdb_conn = settings[type]
    username = settings["username"]
    password = settings["password"]
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
    IDGEN = id_generator()
    MASTER_CUSTOMERS = []
    settings = bb.read_json(settings_file)
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
    elif ARGS["action"] == "execute_ddl":
        mycon = pg_connection()
        execute_ddl('create', mycon)
        mycon.close
    elif ARGS["action"] == "provider_sync":
        get_providers()
    elif ARGS["action"] == "claim_sync":
        get_claims()
    elif ARGS["action"] == "rx_claim_sync":
        get_rx_customers()
    elif ARGS["action"] == "medical_claim_add":
        append_claims()
    elif ARGS["action"] == "rx_claim_add":
        append_rx_claims()
    else:
        print(f'{ARGS["action"]} not found')
    #conn.close()
