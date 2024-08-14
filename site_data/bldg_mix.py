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
            if self.cur_id not in id_map:
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

def init_seed_data(conn):
    ans = {"addr_info": list(conn["sample_restaurants"]["restaurants"].find({},{"_id": 0, "address": 1, "borough": 1})),
           "sites" : generate_sites()
    }
    return ans

def generate_sites(idgen,settings):
    port_ratio = settings["portfolios"]
    site_ratio = settings["sites"]
    batch_size = settings["batch_size"]
    batches = settings["batches"]
    num_to_do = int(batches * batch_size * site_ratio)
    bb.logit(f"Generating Portfolio/Sites - {num_to_do}")
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