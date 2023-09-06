from google.cloud import bigquery 
from flask import Response
import os
import time
import datetime
from pymongo import MongoClient

#GCP Cloud Function!
def claim_polling_trigger(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Runs every 5 seeconds - triggerd buu scheduler
    Calls big query
    loop through results and send to confluent
    """
    client = bigquery.Client()
    print("Logging request: ")
    request_json = request.get_json()
    print(request_json)
    #  Reject if token doesnt match
    if "token" in request_json:
        if request_json["token"] == "sdfjlhag;JH98Hiudhf65HiNug!!":
            print("Token match - success")
        else:
            return Response({'message': 'Authorization token failed'}, status=403, mimetype='application/json')
    else:
        return Response({'message': 'Authorization token failed'}, status=403, mimetype='application/json')
    
    last_check = '2023-05-16 14:30:21'
    tables = ["Provider","Member","Claim","Claim_claimline","Member_address"]
    warehouse_db = "bradybyrd-poc.claims_warehouse"
    mdb_cred = "mongodb+srv://main_admin:<secret>@claims-demo.vmwqj.mongodb.net"
    mdbconn = MongoClient(mdb_cred)
    db = mdbconn["claim_demo"]
    ans = db["preferences"].find_one({"doc_type" : "last_check"})
    last_checked_at = ans["checked_at"]
    max_time = last_checked_at
    orig_checked_at = last_checked_at
    last_check = last_checked_at.strftime("%Y-%m-%d %H:%M:%S")
    processed = []
    new_recs = []
    # Timer will trigger every 2 minutes, for 10 sec operation run 12 times
    for iter in range(3):
        processed = []
        for table in tables:
            tot_processed = 0
            query =   f"""
            select * from `{warehouse_db}.{table}` 
            where modified_at > datetime("{last_check}") 
            order by modified_at DESC 
            limit 100"""
            #print(query)

            query_job = client.query(query)
            curt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            results = query_job.result()  # Waits for job to complete.
            ids = []
            print(f"#---------------- RESULTS {table} ---------------#")
            print(f"{curt}  Run: {last_check}")
            for item in results:
                if item["modified_at"] > max_time:
                    max_time = item["modified_at"]
                doc = dict(item)
                doc["doc_type"] = f"{table}_change"
                #print(doc)
                new_recs.append(doc)
                ids.append(item["provider_id"])
                tot_processed += 1
            answer = ",".join(ids)
            print(answer)
            processed.append({"table" : table, "modified" : tot_processed})
            time.sleep(10)
            last_checked_at = datetime.datetime.now() #last_checked_at + datetime.timedelta(seconds=10)
            last_check = last_checked_at.strftime("%Y-%m-%d %H:%M:%S")
            if len(new_recs) > 0:
                print(f'{tot_processed} records processed')
                db["change_activity"].insert_many(new_recs)
                new_recs = []
        db["preferences"].update_one({"doc_type" : "last_check"},{"$set" : {"checked_at" : last_checked_at}})
        db["activity_log"].insert_one({"activity" : "data polling", "checked_at": orig_checked_at, "processed" : processed})
    return Response({'message': 'successfully connected'}, status=200, mimetype='application/json')

# ---------------------------------- #
#claim_polling_trigger({})