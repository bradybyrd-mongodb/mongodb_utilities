import sys
import os
from collections import OrderedDict
import json
import random
import time
from bson.objectid import ObjectId
import datetime
from pymongo import MongoClient
from faker import Faker

fake = Faker()

class Signal:
    # Use an instance to track the current record being processed
    def __init__(self, details = {}):
        self.variation = 0.1
        self.signal_type = {"type": "room_temp","avg" : 65, "unit" : "degrees"}
        self.device_types = [
            {"type": "chiller", "avg" : 55, "unit" : "degrees"},
            {"type": "air_handler", "avg" : 230, "unit" : "cfm"},
            {"type": "boiler", "avg" : 180, "unit" : "degrees"},
            {"type": "vav", "avg" : 85, "unit" : "degrees"},
            {"type": "air_handler","avg" : 35, "unit" : "bars"},
            {"type": "fan","avg" : 2600, "unit" : "rpm"},
            {"type": "room_temp","avg" : 65, "unit" : "degrees"},
            {"type": "set_point","avg" : 90, "unit" : "percent"},
            {"type": "particle_density","avg" : 7, "unit" : "per-litre"},
            {"type": "carbon_monoxide","avg" : 2, "unit" : "ppm"},
            {"type": "carbon_dioxide","avg" : 28, "unit" : "ppm"},
            {"type": "power","avg" : 40, "unit" : "amps"},
            {"type": "distribution","avg" : 400, "unit" : "amps"},
            {"type": "backup_generator","avg" : 55, "unit" : "amps"},
            {"type": "air_pressure","avg" : 230, "unit" : "cfm"},
            {"type": "solar_panel","avg" : 180, "unit" : "voltage"},
            {"type": "solar_regulator","avg" : 85, "unit" : "efficiency"},
            {"type": "back_pressure","avg" : 35, "unit" : "bars"},
            {"type": "water_pump","avg" : 2600, "unit" : "rpm"},
            {"type": "water_pressure","avg" : 65, "unit" : "psi"},
            {"type": "battery_status","avg" : 90, "unit" : "percent"},
            {"type": "heat_calling","avg" : 100, "unit" : "rooms"},
            {"type": "cooling_calling","avg" : 100, "unit" : "rooms"}
        ]
        
    def get_items(self):
        cur = datetime.datetime.now()
        random.seed(int(cur.microsecond/1000))
        result = []
        for item in self.device_types:
            result.append({"signal" : item["type"], "value": self.cur_item_value(item), "timestamp" : fake.past_datetime(start_date="-5m")})
        return(result)

    def cur_item(self):
        return(self.signal_type)
    
    def cur_item_value(self, curitem):
        cur = curitem["avg"]
        low = cur * (1 - self.variation)
        high = cur * (1 + self.variation)
        pnt = random.randint(1,99)/100
        return((high - low) * pnt + low)

class Locale:
    # Use an instance to track the current record being processed
    def __init__(self, details = {}):
        self.variation = 0.1
        self.details = {"customer": "C-1000000"}
        self.idgen = None
        self.parents = [
            {"dtype": "floor", "range": [3,30]},
            {"dtype": "room", "range": [201,999]},
            {"dtype": "edge-device", "range": [3,30]},
        ]
        if "idgen" in details:
            self.idgen = details["idgen"]
        
    def get_items(self):
        cur = datetime.datetime.now()
        random.seed(int(cur.microsecond/1000))
        result = []
        for item in self.device_types:
            result.append({"dtype": "site", "value" : self.idgen.random_value("S-")})
        return(result)

class AssetDetails:
    # Use an instance to track the current record being processed
    def __init__(self, details = {}):
        self.asset = {}
        self.counter = 0
        self.idgen = None
        self.asset_types = [
            {"dtype": "floor", "range": [3,30]},
            {"dtype": "room", "range": [201,999]},
            {"dtype": "edge-device", "range": [3,30]},
        ]
        if "idgen" in details:
            self.idgen = details["idgen"]
        
    def asset_info(self, seq):
        fake.random_element(('Floor','Room','Edge-device'))

    def get_assets(self):
        # Try to generate the array of detail values to describe the parentage 
        ans = []
        loop_size = 2
        if self.counter % 100 == 0:
            loop_size = 3
        for inc in range(loop_size):
            #print(f'INC: {inc}, Loop: {loop_size}, cnt: {self.counter}')
            typ = self.asset_types[inc]["dtype"]
            pair = self.asset_types[inc]["range"]
            val = random.randint(pair[0],pair[1])
            if inc == 1:
                val = f"B-{val}"
            elif inc == 2:
                val = self.idgen.random_value("A-")
            ans.append({"dtype": typ, "value" : val})
        self.counter += 1
        return(ans)

class CurItem:
    # Use an instance to track the current record being processed
    def __init__(self, details = {}):
        self.addr_info = {}
        self.sites = []
        self.id_map = {}
        self.cur_id = False
        self.ipos = 0
        self.version = "1.0"
        self.counter = 0
        if "addr_info" in details:
            self.addr_info = details["addr_info"]
        if "sites" in details:
            self.sites = details["sites"]
        if "version" in details:
            self.version = details["version"]

    def set_addr_info(self, info):
        self.addr_info = info

    def set_cur_id(self, id_val):
        #for each record generated, store the current id as a local value so you can lookup multiple 
        #items
        self.cur_id = id_val
        if self.counter >= len(self.sites) - 1:
            self.counter = 0 
        else:
            self.counter += 1
        self.ipos += 1
        return(id_val)

    def get_site(self):
        return self.sites[self.counter] 

    def get_item(self, i_type = "none", passed_id = None):
        item = None
        ans = None
        if passed_id is not None:
            self.cur_id = passed_id
            #item = self.addr_info[self.id_map[passed_id]]
        try:
            if self.cur_id not in self.id_map:
                item = self.addr_info[self.ipos]
                self.ipos += 1
            else:
                item = self.addr_info[self.id_map[self.cur_id]]
            if i_type == "none":
                ans = item
            elif i_type == "portfolio_id":
                ans = self.sites[self.counter]["portfolio_id"]
            elif i_type == "portfolio_name":
                ans = self.sites[self.counter]["portfolio_name"]
            elif i_type == "site_id":
                ans = self.sites[self.counter]["site_id"]
            elif i_type == "site_name":
                ans = self.sites[self.counter]["site_name"]
            elif i_type == "version":
                ans = self.version
            else:
                ans = item["address"][i_type]
        except Exception as e:
            print("---- ERROR --------")
            print("---- Vals --------")
            print(f'Type: {i_type}, Counter: {self.counter}, pos: {self.ipos}')
            print("---- error --------")
            print(e)
            exit(1)
        return ans

def init_seed_data(conn, idgen, settings):
    ans = {"addr_info": list(conn["sample_restaurants"]["restaurants"].find({},{"_id": 0, "address": 1, "borough": 1})),
           "sites" : generate_sites(idgen, settings)
    }
    return ans

def generate_sites(idgen,settings):
    port_ratio = settings["portfolios"]
    site_ratio = settings["sites"]
    batch_size = settings["batch_size"]
    batches = settings["batches"]
    num_to_do = int(batches * batch_size * site_ratio)
    print(f"# - Generating Portfolio/Sites - {num_to_do}")
    ans = []
    for k in range(num_to_do):
        if k % 10 == 0:
            cur_portfolio_id = idgen.get("P-")
            cur_portfolio = fake.company()
        ans.append({
            "portfolio_id" : cur_portfolio_id,
            "portfolio_name" : cur_portfolio,
            "site_id" : idgen.get("S-"),
            "site_name" : fake.street_name()
        })
    return ans

def get_measurements(target_type, item_type = "chiller"):
    if target_type != "mongo":
        return "placeholder"
    icnt = 12 * 24
    base_time = datetime.datetime.now() - datetime.timedelta(days = 1)
    arr = []
    for k in range(icnt):
        arr.append({
            "timestamp" : base_time + datetime.timedelta(seconds = 300 * k),
            "temperature" : random.randint(60,80),
            "rotor_rpm" : random.randint(1200,3500),
            "input_temp" : random.randint(45,70),
            "output_temp" : random.randint(42,50),
            "output_pressure" : random.randint(60,110),
            "alarm" : fake.random_element(('no', 'no', 'no','no','no','yes','no'))
        })
    return(arr)

def get_building(settings):
    prefix = "B-"
    base = settings["base_counter"]
    tot = settings["batch_size"] * settings["batches"] * settings["process_count"]
    val = random.randint(base, base + tot)
    return(f'{prefix}{val}')

def get_asset(settings):
    prefix = "A-"
    base = settings["base_counter"]
    tot = settings["batch_size"] * settings["batches"] * settings["process_count"] * 20
    val = random.randint(base, base + tot)
    return(f'{prefix}{val}')

