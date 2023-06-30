import sys
import os
base_dir = os.path.dirname(os.path.abspath(__file__))
# apppend parent folder to path
sys.path.append(os.path.dirname(base_dir))
from bbutil import Util
from id_generator import Id_generator
from faker import Faker
import datetime
import random

fake = Faker()

'''
    Model Loader
    3/1/23 BJB

    This module holds all the document models and synth data generation
'''
def comm_types():
    return [
        "email",
        "phone",
        "twitter",
        "call",
        "facebook",
        "instagram"
    ]

def generate(icnt,params):
    func = globals()[params["model"]]
    result = func(icnt,params)
    return result

def cif(icnt,params):
    age = random.randint(28,84)
    year = 2020 - age
    month = random.randint(1,12)
    day = random.randint(1,28)
    name = fake.name()
    doc = {}
    doc['profile_id'] = params["id"]
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
    doc["recommendations"] = []
    doc["recent_purchases"] = []
    doc["version"] = "1.0"
    return(doc)
