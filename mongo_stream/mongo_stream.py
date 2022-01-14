'''
#---------- Instructions ---------------#
-- Make sure libraries are installed:
   pip3 install pymongo dnspython
-- To Run:
   python3 mongo_stream.py --f <settings file>
-- Default settings file name
   mongo_stream_settings.json

To suppress SSL Verify add the follwoing into a connection string: &ssl_cert_reqs=CERT_NONE

Sample Output:
#     Scanning Changes for cluster:  clinical
{'_id': {'_data': '8261BA3DB0000000012B022C0100296E5A1004175D21F922A34BD1BDD3E00939C3B23946645F6964006461BA3D876D35EFACFD55E8A60004'}, 'operationType': 'insert', 'clusterTime': Timestamp(1639595440, 1), 'fullDocument': {'_id': ObjectId('61ba3d876d35efacfd55e8a6'), 'pos': '86675', 'chrom': 'chr3'}, 'ns': {'db': 'clinical', 'coll': 'results'}, 'documentKey': {'_id': ObjectId('61ba3d876d35efacfd55e8a6')}}
'''

from io import BufferedIOBase
from pymongo import MongoClient
from pymongo import UpdateOne, InsertOne, DeleteOne, UpdateOne, ReplaceOne
import os
import sys
import traceback
import pprint
import argparse
import json
from time import time
from datetime import datetime

class Util:
    def __init__(self, details = {}):
        self.hfile = False
        self.logfile = False
        self.logfilename = "mongo_stream_log.txt"

    def logit(self, message, log_type = "INFO", display_only = True):
        cur_date = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        stamp = f"{cur_date}|{log_type}> "
        if type(message) == dict or type(message) == list:
            message = str(message)
        if log_type == "raw":
            message = "Raw output, check file"
        for line in message.splitlines():
            print(f"{stamp}{line}")
        self.file_log(message)

    def message_box(self, msg, mtype = "sep"):
        tot = 100
        start = ""
        res = ""
        msg = msg[0:84] if len(msg) > 85 else msg
        ilen = tot - len(msg)
        if (mtype == "sep"):
            start = f'#{"-" * int(ilen/2)} {msg}'
            res = f'{start} {"-" * (tot - len(start) + 1)}#'
        else:
            res = f'#{"-" * tot}#\n'
            start = f'#{" " * int(ilen/2)} {msg} '
            res += f'{start}{" " * (tot - len(start) + 1)}#\n'
            res += f'#{"-" * tot}#\n'
        self.logit(res)
        return res

    def read_json(self, json_file, is_path = True):
        result = {}
        if is_path:
            with open(json_file) as jsonfile:
                result = json.load(jsonfile)
        else:
            result = json.loads(json_file)
        return result

    def file_log(self, content, action = "none"):
        cur_date = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        stamp = f"{cur_date}|I> "
        with open(self.logfilename, 'a') as lgr:
            lgr.write(f'{stamp}{content}\n')

def watchCluster(streamCon, destCon, settings):
   # we can specify operation types as well as other conditions here
   pipeline = [{'$match': {'operationType': {'$in': ['update', 'insert', 'replace', 'delete']}}}]

   loggingDB = settings["loggingDB"]
   loggongColl = settings["logCollection"]
   tokenCollection = settings["tokenCollection"]
   cluster = settings["cluster"]
   exclusion_list = settings["exclusionList"]

   bb.message_box(f"Scanning Changes for cluster:  {cluster}", "title")
   try:
       # Set the resume token name to cluster.db.collection
       tokenId = cluster

       # resume token is how we pick up where we left off on restart.
       # In this example, we're also storing it in the dest cluster.
       resumeToken = None
       # fetch last resume token - may not be present in which case we start from the beginning
       rt = destCon[loggingDB][tokenCollection].find_one({"_id": tokenId})
       if rt is None:
           # create token entry
           destCon[loggingDB][tokenCollection].insert_one({"_id": tokenId, 'value': None})
       else:
           resumeToken = rt['value']
       # start the watch process - note the 'updateLookup' option here
       # which returns the full document to the watch consumer (this program)
       with streamCon.watch(pipeline, full_document='updateLookup', resume_after=resumeToken, batch_size=settings["batchSize"]) as stream:
           cnt = 1
           last_ts = 0
           print_time = time()
           bulk_time = time()
           start_time = datetime.now()
           bulk_changes = {}
           log_changes = []
           bulk_cnt = settings["bulkCount"]
           while stream.alive:
               u = stream.try_next()
               resume_token = stream.resume_token
               time_diff = (datetime.now() - start_time)
               execution_time = time_diff.total_seconds()
               bb.logit(f'TotalLoop: {execution_time} seconds')
               start_time = datetime.now()

               if u is not None:
                   if cnt == 0 or (time() - print_time >= 1):
                       v = streamCon["local"]["oplog.rs"].find({}, {"_id":0,"ts":1}).sort([("$natural",-1)]).limit(1).next()
                       last_ts = u["clusterTime"].time
                       millis = v["ts"].time-last_ts
                   db = u['ns']['db']
                   coll = u['ns']['coll']
                   ns = db + '.' + coll
                   if ns not in exclusion_list:
                       cnt += 1
                       if ns not in bulk_changes:
                           bulk_changes[ns] = []
                       if u['operationType'] in ['update', 'replace']:
                           # Upsert doc dest
                           if u['fullDocument'] is not None:
                               #bulk_changes[ns].append({"op" : "replace", "query" : {'_id': u['fullDocument']['_id']}, "doc" : u['fullDocument']})
                               bulk_changes[ns].append(ReplaceOne({'_id': u['fullDocument']['_id']}, u['fullDocument'], upsert=True))
                               #destCon[db][coll].replace_one({'_id': u['fullDocument']['_id']}, u['fullDocument'], upsert=True)
                           else:
                               u['fullDocument'] = "ERROR Missing document"
                       elif u['operationType'] in ['insert']:
                           if u['fullDocument'] is not None:
                               bulk_changes[ns].append(InsertOne(u['fullDocument']))
                               #bulk_changes[ns].append({"op" : "insert", "doc" : u['fullDocument']})
                               #destCon[db][coll].insert_one(u['fullDocument'])
                           else:
                               u['fullDocument'] = "ERROR Missing document"
                       elif u['operationType'] == 'delete':
                           # receive the original document
                           full_doc = destCon[db][coll].find_one(u['documentKey'])
                           if full_doc == None:
                               full_doc = u['documentKey']
                           u["fullDocument"] = full_doc

                           # remove from doc dest
                           #destCon[db][coll].delete_one(u['documentKey'])
                           bulk_changes[ns].append(DeleteOne(u['documentKey']))
                           #bulk_changes[ns].append({"op" : "delete", "id" : u['documentKey']})

                       if settings["enableLogging"] == "Yes":
                           log_changes.append({"timestamp" : datetime.now(), "op" : u['operationType'], "collection" : coll, "document" : u['fullDocument']})
                           #destCon[loggingDB][loggongColl].insert_one({"timestamp" : datetime.now(), "op" : u['operationType'], "collection" : coll, "document" : u['fullDocument']})

                       # just to show the changestream object - comment out for less noise
                       # print(f'Modifying: {coll}, op: {u["operationType"]}, Id: {u["fullDocument"]["_id"]}')
                   if cnt > bulk_cnt or (cnt>0 and (time()-bulk_time>=10)):
                       cnt = 0
                       bulk_operate(destCon, bulk_changes)
                       bulk_changes = {}
                       bulk_operate(destCon, log_changes, "log")
                       log_changes = []
                       bb.logit(f'Applying {cnt} batch of changes')
                       bulk_time = time()
                   end_time = datetime.now()
                   time_diff = (end_time - start_time)
                   execution_time = time_diff.total_seconds()
                   bb.logit(f'Local save: {execution_time} seconds')


               else:
                   if cnt > 0:
                       millis = 0
                       cnt = 0
                       bulk_operate(destCon, bulk_changes)
                       bulk_changes = {}
                       bulk_operate(destCon, log_changes, "log")
                       log_changes = []
                       bb.message_box("All changes applied waiting for the next block of changes")
                       bulk_time = time()

               # Update resume token
               destCon[loggingDB][tokenCollection].update_one({"_id": tokenId}, {'$set': {'value': resume_token}})
               if (time() - print_time >= 10):
                   bb.message_box(f"Replication lag is {millis} milliseconds")
                   print_time = time()
   except Exception as err:
       exc_type, exc_obj, exc_tb = sys.exc_info()
       fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
       print(f'ERROR: {exc_type} in {fname} on line {exc_tb.tb_lineno}')
       traceback.print_exc()
       print(err)
       if u is not None:
           print("--offending oplog--")
           print(u)

def bulk_operate(conn, bulker, type="bulk"):
    if len(bulker) == 0:
        return
    result = ""
    if type == "log":
        db = settings["loggingDB"]
        coll = settings["logCollection"]
        result = conn[db][coll].insert_many(bulker)
        bb.logit("#-- Bulk Update Result --#")
        pprint.pprint(result)
    else:
        for ns in bulker:
            parts = ns.split(".")
            db = parts[0]
            coll = parts[1]
            result = conn[db][coll].bulk_write(bulker[ns])
            bb.logit("#-- Bulk Update Result --#")
            pprint.pprint(result)



def run_tester(settings):
   batchSize = settings["batchSize"]
   cluster = settings["cluster"]

   bb.message_box(f"Scanning Changes for cluster:  {cluster}", "title")

   pipeline = [{'$match': {'operationType': {'$in': ['update', 'insert', 'replace', 'delete']}}}]
   resumeToken = None

   try:
       streamCon = MongoClient(
           host=settings["source_uri"],
           serverSelectionTimeoutMS=2000,
           socketTimeoutMS=500,
           connectTimeoutMS=500
       )

       with streamCon.watch(pipeline, full_document='updateLookup', resume_after=resumeToken, batch_size=batchSize) as stream:
           for u in stream:
               print(f'Modifying: {u["ns"]["coll"]}, op: {u["operationType"]}')
               print(u)

       streamCon.close()

   except Exception as err:
       print('Error occurred:' + str(err))
       sys.exit(1)

def run(settings):
   try:
       streamCon = MongoClient(
           host=settings["source_uri"],
           serverSelectionTimeoutMS=settings["serverSelectionTimeoutMS"],
           socketTimeoutMS=settings["socketTimeoutMS"],
           connectTimeoutMS=settings["connectTimeoutMS"]
       )

       destCon = MongoClient(
           host=settings["dest_uri"],
           serverSelectionTimeoutMS=settings["serverSelectionTimeoutMS"],
           socketTimeoutMS=settings["socketTimeoutMS"],
           connectTimeoutMS=settings["connectTimeoutMS"]
       )

       watchCluster(streamCon, destCon, settings)

       streamCon.close()
       destCon.close()

   except Exception as err:
       print('Error occurred:' + str(err))
       sys.exit(1)

def init():
   parser = argparse.ArgumentParser(description="MongoStream")
   parser.add_argument('-f', '--file', type=str, help="Settings file", required=False, default="mongo_stream_settings.json")
   args = parser.parse_args()
   settings_file = args.file

   print("\n")
   bb.message_box("Using settings file {settings_file}")

   settings = bb.read_json(settings_file)

   if settings["exclusionList"] == None:
       exclusion_list = []
   else:
       exclusion_list = [s.strip() for s in settings["exclusionList"].split(",")]
   exclusion_list.append(settings["loggingDB"] + '.' + settings["tokenCollection"])
   exclusion_list.append(settings["loggingDB"] + '.' + settings["logCollection"])
   settings["exclusionList"] = exclusion_list

   if settings["batchSize"] < 100 or settings["batchSize"] > 1000:
       settings["batchSize"] = 1000
   print(settings)
   print("\n")

   return settings

if __name__ == "__main__":
   global bb
   bb = Util()

   settings = init()
   #run_tester(settings)
   run(settings)
