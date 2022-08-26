'''
    Template for Communication Message profile
    BJB 8/18/22
    include this file in your python header
    import CommFactory as comm
'''
from faker import Faker
import random
import datetime
import copy
import pprint
fake = Faker()
'''
    "communicationid": "42~MECFFVHS^HMO^66475^Other^Verizon - Medicare Cal^UKN-00000000^CPLVoyantFax",
    "constituentid": "42~MECFFVHS",
  
    Campaigns - each campaign generates x1000 messages
        compaignidentifier, campaignname, cmnctnmgrcampaignname

'''
class CommFactory:
    categories = ["Running", "Cycling","CrossFit","OrangeTheory","Walking","Swimming","Jujitsu","SpeedKnitting"]
    channels = ["SMS","FAX","SMS","IVR", "Pigeon"]
    cell_providers = ["Verizon", "AT&T","T-Mobile","Cricket"]
    metrics = ["duration","steps","energy_burned","distance","max_heart_rate","elevation_change","blood_glucose"]
    makers = ["apple","fitbit","oura","samsung","roche","omnipod","B-D"]
    campaigns = []

    def __init__(self, details = {}):
        oddvar = "odd"
        build_campaigns()

    def build_doc(self, id_val, profile, doc):
        doc["id"] = id_val
        doc["vendor"] = random.choice(self.vendors)
        #Build a few data elements
        data_tag = []
        num_datas = random.randint(1,5)
        letval = random.randint(65,91)
        for icnt in range(num_datas):
            data_tag.append(self.build_data(f'{id_val}-{chr(letval)}', profile, copy.deepcopy(doc["data"][0])))
            letval += 1
        doc["data"] = data_tag
        doc["meta"]["count"] = num_datas + 1
        doc["version"] = "1.0"
        return(doc)


    def build_data(self, id_val, profile, doc):
        age = random.randint(0,6)
        yr = 0
        month = random.randint(1,12)
        month = month - age
        if month < 1:
            month = 12 + month
            yr = 1
        year = 2022 - yr
        day = random.randint(1,28)
        #pprint.pprint(profile)
        doc["id"] = id_val
        doc["category"] = random.choice(self.categories)
        doc["created_at"] = datetime.datetime(year,month,day, 10, 45)
        doc["deleted_at"] = None
        doc["end_time"] =  doc["created_at"] + datetime.timedelta(hours=1)
        doc["log_id"] = "43050850237"
        doc["source"]["device"]["manufacturer"] = random.choice(self.makers)
        doc["source"]["device"]["model"] = fake.bs()
        doc["user"]["organization_id"] = profile["organization_id"]
        doc["user"]["organization_id"] = profile["uid"]
        doc["user"]["user_id"] = profile["profile_id"]
        doc["user_notes"][0]["type"] = fake.bs()
        doc["user_notes"][0]["value"] = fake.word()
        doc["version"] = "1.1"
        for it in range(2):
            doc["source"]["device"]["diagnostics"][it]["value"] = fake.word()

        mets = []
        for k in range(random.randint(0,3)):
            mets.append(self.build_metric(self.metrics[k]))
        doc["metrics"] = mets
        return(doc)       

    def communication_id_gen(self, id_val):
        # "42~MECFFVHS^HMO^66475^Other^Verizon - Medicare Cal^UKN-00000000^CPLVoyantFax"
        return f'{random.randint(10,100)}~{id_val}^HMO^66475^Other^{random.choice(cell_providers)} - Medicare Cal^UKN-{id_val}'

    def build_campaigns():
        for k in range(100):
            campaigns.append(fake.bs())

    def build_metric(self, item = "none"):
        if item == "none":
            item = random.choice(self.metrics)
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
