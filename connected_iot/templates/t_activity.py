'''
    Template for customer profile
    BJB 8/2/22
    include this file in your python header
    import profile as profile
'''
from faker import Faker
import random
import datetime
import copy
import pprint
fake = Faker()

categories = ["Running", "Cycling","CrossFit","OrangeTheory","Walking","Swimming","Jujitsu","SpeedKnitting"]
vendors = ["WellDoc","Oura","FitBit","Apple", "QuantHealth"]
metrics = ["duration","steps","energy_burned","distance","max_heart_rate","elevation_change","blood_glucose"]
makers = ["apple","fitbit","oura","samsung","roche","omnipod","B-D"]
# Three scenarios - 
# 1. build a new doc with some metrics
# 2. Add to an existing doc with additional metrics (same activity)
# 3. Add a whole new activity to the same data element

def build_doc(id_val, curdoc, act_id = ""):
    doc = {}
    doc["id"] = id_val
    doc['profile_id'] = curdoc["profile_id"]
    doc["uid"] = curdoc["uid"]
    doc["vendor"] = random.choice(vendors)
    if act_id != "":
        crit = {"id" : act_id}
        doc = {"$addToSet" : {"data" : build_data(id_val, act_id)}}
    doc["data"] = build_data(id_val, act_id)
    doc["version"] = "1.0"
    return(doc)

def build_data(id_val, act_id):
    doc = {}
    age = random.randint(0,6)
    yr = 0
    month = random.randint(1,12)
    month = month - age
    if month < 1:
        month = 12 + month
        yr = 1
    year = 2022 - yr
    day = random.randint(1,28)
    doc["id"] = id_val
    doc["category"] = {"string" : "running"}
    doc["checksum"] = "963133bb47aea1405d32cd1"
    doc["created_at"] = datetime.datetime(year,month,day, 10, 45)
    doc["deleted_at"] = None
    doc["end_time"] =  doc["created_at"] + datetime.timedelta(hours=1)
    doc["log_id"] = "43050850237"
    mets = []
    if act_id == "":
        terms = metrics[0:2]
    else:
        terms = [random.choice(metrics)]
    for k in terms:
        mets.append(build_metric(k))
    doc["metrics"] = mets

def build_doc_t(id_val, curdoc, doc):
    doc["id"] = id_val
    doc["vendor"] = random.choice(vendors)
    #Build a few data elements
    data_tag = []
    num_datas = random.randint(1,5)
    for icnt in range(num_datas):
        data_tag.append(build_data_t(id_val, curdoc, copy.deepcopy(doc["data"][0])))
    doc["data"] = data_tag
    doc["meta"]["count"] = num_datas + 1
    doc["version"] = "1.0"
    return(doc)


def build_data_t(id_val, cur_doc, doc, act_id = ""):
    age = random.randint(0,6)
    yr = 0
    month = random.randint(1,12)
    month = month - age
    if month < 1:
        month = 12 + month
        yr = 1
    year = 2022 - yr
    day = random.randint(1,28)
    #pprint.pprint(cur_doc)
    doc["id"] = id_val
    doc["category"] = random.choice(categories)
    doc["created_at"] = datetime.datetime(year,month,day, 10, 45)
    doc["deleted_at"] = None
    doc["end_time"] =  doc["created_at"] + datetime.timedelta(hours=1)
    doc["log_id"] = "43050850237"
    doc["source"]["device"]["manufacturer"] = random.choice(makers)
    doc["source"]["device"]["model"] = fake.bs()
    doc["user"]["organization_id"] = cur_doc["organization_id"]
    doc["user"]["organization_id"] = cur_doc["uid"]
    doc["user"]["user_id"] = cur_doc["profile_id"]
    doc["user_notes"][0]["type"] = fake.bs()
    doc["user_notes"][0]["value"] = fake.word()

    for it in range(2):
        doc["source"]["device"]["diagnostics"][it]["value"] = fake.word()

    mets = []
    if act_id == "":
        terms = metrics[0:2]
    else:
        terms = [random.choice(metrics)]
    for k in terms:
        mets.append(build_metric(k))
    doc["metrics"] = mets
    return(doc)       

def build_metric(item = "none"):
    if item == "none":
        item = random.choice(metrics)
    answer = {}
    
    if item == "duration":
        answer = {"type": item, "value" : random.randint(1,12), "unit" : "sec"}
    elif item == "distance":
        answer = {"type": item, "value" : random.randint(1,10000), "unit" : "m"}
    elif item == "max_heart_rate":
        answer = {"type": item, "value" : random.randint(100,165), "unit" : "bpm"}
    elif item == "steps":
        answer = {"type": item, "value" : random.randint(1000,20000), "unit" : "steps"}
    elif item == "energy_burned":
        answer = {"type": item, "value" : random.randint(500,3000), "unit" : "kcal"}
    elif item == "blood_glucose":
        answer = {"type": item, "value" : random.randint(90,260), "unit" : "mg/dL"}
    elif item == "elevation_change":
        answer = {"type": item, "value" : random.randint(100,2000), "unit" : "sec"}

    answer["origin"] = "manual"
    return(answer)

'''
    {
        "profile_id" : "blahblah697980",
        "vendor" : "Welldoc",
        "data" : [
          {
            "category" : {"string" : "running"},
            "checksum" : "963133bb47aea1405d32cd1",
            "created_at" : "2020-05-13T10:50:16.015573",
            "deleted_at" : "2022-07-27T10:50:16.015573",
            "end_time" : "2022-07-27T10:50:16.015573",
            "id" : "BAX-9999",
            "log_id" : "43050850237",
            "metrics" [
              {
                "origin" : "manual",
                "type" : "distance",
                "unit" : "m",
                "value" : 3218.688
              },
              {
                "origin" : "manual",
                "type" : "active_duration",
                "unit" : "s",
                "value" : 1800
              },
              {
                "origin" : "manual",
                "type" : "steps",
                "unit" : "count",
                "value" : 2755
              },
              {
                "origin" : "manual",
                "type" : "energy_burned",
                "unit" : "kcal",
                "value" : 3218.688
              }         
            ]
          }
        ]
      }
'''