'''
    Template for customer profile
    BJB 8/2/22
    include this file in your python header
    import profile as profile
'''
from faker import Faker
import random
import datetime
fake = Faker()

class ProfileFactory:
    def __init__(self, details = {}):
        self.organizations = self.make_organizations()

    def make_organizations(self):
        orgs = []
        for k in range(50):
            orgs.append({"vendor" : f'{fake.word()}_{fake.word()}', "organization_id" : fake.uuid4()})
        return(orgs)

    def build_doc(self, idval):     
        age = random.randint(28,84)
        year = 2020 - age
        month = random.randint(1,12)
        day = random.randint(1,28)
        name = fake.name()
        doc = {}
        doc["uid"] = fake.uuid4()
        org = self.organizations[random.randint(0,49)]
        doc["vendor"] = org["vendor"]
        doc["organization_id"] = org["organization_id"]
        doc['profile_id'] = idval
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
        doc["version"] = "1.0"
        return(doc)
