# ----------------------------------------------------------------------- #
#  Performance Queries for CommTracker
'''
$sample disease field
create regex combinations from the terms
execute the queries

'''
import sys
import os
import json
import datetime
import time
import pprint
from pymongo import MongoClient
base_dir = os.path.dirname(os.path.abspath(__file__))
# apppend parent folder to path
sys.path.append(os.path.dirname(base_dir))
sys.path.append(os.path.join(base_dir, "templates"))
from bbutil import Util
import commtracker_queries as qq

class PerfQueries:
    def __init__(self, details = {}):
        self.args = details["args"]
        self.settings = details["settings"]
        self.db = details["db"]
        self.bb = Util()

    def perform_action(self, item, db):
        #self.bb.logit(f'Performing: {item}')
        try:
            start = datetime.datetime.now()
            if qq.queries[item]["type"] == "agg":
                output = db.emr.aggregate(qq.queries[item]["query"])
            elif qq.queries[item]["type"] == "insert":
                output = db.emr.insert(qq.queries[item]["query"])
            elif qq.queries[item]["type"] == "update":
                output = db.emr.update(qq.queries[item]["query"],qq.queries[item]["update"])
            end = datetime.datetime.now()
            elapsed = end - start
            secs = (elapsed.seconds) + elapsed.microseconds * .000001
            self.bb.logit(f"Performing {item} - Elapsed: {secs}")
        except KeyboardInterrupt:
            print("ERROR - canceled")
            sys.exit(0)
        except Exception as e:
            print(f'{datetime.datetime.now()} - DB-CONNECTION-PROBLEM: ')
            print(f'{str(e)}')
            connect_problem = True

    def build_query(self, query_params):
        pipe = query_params["query"]
        coll = self.settings["collection"]
        if "collection" in query_params:
            coll = query_params["collection"]
        docs = 0
        if query_params["type"] == "agg":
            cursor = self.db[coll].aggregate(pipe)
        if query_params["type"] == "find":
            if "project" in query_params:
                self.bb.logit(f'Query: {pipe}')
                self.bb.logit(f'Project: {query_params["project"]}')
                if "limit" in query_params:
                    cursor = self.db[coll].find(pipe,query_params["project"]).limit(query_params["limit"])
                else:
                    cursor = self.db[coll].find(pipe,query_params["project"])
            elif "limit" in query_params:
                cursor = self.db[coll].find(pipe).limit(query_params["limit"])
            elif "count" in query_params:
                cursor = self.db[coll].count_documents(pipe)
            else:
                cursor = self.db[coll].find(pipe)
        return(cursor)

    def query_list(self, items):
        #self.bb.logit(f'Performing: {item}')
        result = []
        for item in items:
            start = datetime.datetime.now()
            if not item in qq.queries:
                self.bb.logit(f"Cant find: {item} skipping")
                continue
            details = qq.queries[item]
            output = self.build_query(details)
            bulker = []
            bulk_cnt = 0
            docs = 0
            if "count" in details:
                docs = output
            else:
                for k in output:
                    #print(k)
                    bulker.append(k)
                    if "numrecords" in k:
                        docs = k["numrecords"]
                    else:
                        docs += 1
                        if (docs % 1000) == 0:
                            print(f'{docs}.', end="", flush=True)
                            if bulk_cnt > 20:
                                #self.bb.file_log(pprint.pformat(bulker),"new")
                                bulk_cnt = 0
                            else:
                                #self.bb.file_log(pprint.pformat(bulker))
                                bulk_cnt += 1
                            bulker = []

            end = datetime.datetime.now()
            elapsed = end - start
            cur_date = end.strftime("%m/%d/%Y %H:%M:%S")
            secs = (elapsed.seconds) + elapsed.microseconds * .000001
            self.bb.logit(f"Performing {item} - Elapsed: {format(secs,'f')} - Docs: {docs}")
            self.bb.logit(f'Operation: {details["type"]}, Query:')
            self.bb.logit(details["query"])
            result.append(f"{cur_date},{item},{format(secs,'f')},{docs}\n")
                
            self.bb.logit("------------------------------------")
        return(result)

    def perf_stats(self):
        iters = 1
        self.bb.message_box("Query Performance", "title")
        start_time = datetime.datetime.now()
        logfilename = "perflog.txt"
        items = [
            "simple"
        ]
        if "batch" in self.args:
            batch = self.args["batch"]
            items = qq.batches[batch]
        if "iters" in self.args:
            iters = int(self.args["iters"])
        with open(logfilename, 'a') as lgr:
            lgr.write("#------------------------------- Performance Run Log -----------------------------#\n")
            lgr.write("Date,Name,elapsed,num_docs\n")
            for k in range(iters):
                result = self.query_list(items)
                for line in result:
                    lgr.write(line)
            
        end_time = datetime.datetime.now()
        time_diff = (end_time - start_time)
        execution_time = time_diff.total_seconds()
        self.bb.logit(f"Execution total took {execution_time} seconds")
        self.bb.logit("-- Complete --")
