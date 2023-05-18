from google.cloud import bigquery 
#from flask import Response
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
    '''request_json = request.get_json()
    print(request_json)
    #  Reject if token doesnt match
    if "token" in request_json:
        if request_json["token"] == "sdfjlhag;JH98Hiudhf65HiNug!!":
            print("Token match - success")
        else:
            return Response({'message': 'Authorization token failed'}, status=403, mimetype='application/json')
    else:
        return Response({'message': 'Authorization token failed'}, status=403, mimetype='application/json')
    '''
    last_check = '2023-05-16 14:30:21'
    warehouse_db = "bradybyrd-poc.claims_warehouse.Provider"
    mdb_cred = "mongodb+srv://main_admin:bugsyBoo@claims-demo.vmwqj.mongodb.net"
    mdbconn = MongoClient(mdb_cred)
    db = mdbconn["claim_demo"]
    ans = db["preferences"].find_one({"doc_type" : "last_check"})
    last_checked_at = ans["checked_at"]
    start_check_at = last_checked_at
    last_check = last_checked_at.strftime("%Y-%m-%d %H:%M:%S")
    tot_processed = 0
    new_recs = []
    # Timer will trigger every 2 minutes, for 10 sec operation run 12 times
    for iter in range(3):
        query =   f"""
        select * from `{warehouse_db}` 
        where modified_at > datetime("{last_check}") 
        order by modified_at DESC 
        limit 100"""
        print(query)

        query_job = client.query(query)

        results = query_job.result()  # Waits for job to complete.
        ids = []
        print("#---------------- RESULTS ---------------#")
        print(f"  run: {last_check}")
        for item in results:
            doc = dict(item)
            doc["doc_type"] = "provider_change"
            new_recs.append(doc)
            ids.append(item["member_id"])
            tot_processed += 1
        answer = ",".join(ids)
        print(answer)
        time.sleep(10)
        last_checked_at = last_checked_at + datetime.timedelta(seconds=10)
        last_check = last_checked_at.strftime("%Y-%m-%d %H:%M:%S")
        db["change_activity"].insert_many(new_recs)
    db["preferences"].update_one({"doc_type" : "last_check"},{"$set" : {"checked_at" : last_checked_at}})
    db["activity_log"].insert_one({"activity" : "data polling", "checked_at": orig_checked_at, "processed" : tot_processed})
    return Response({'message': 'successfully connected'}, status=200, mimetype='application/json')

# ---------------------------------- #
claim_polling_trigger({})