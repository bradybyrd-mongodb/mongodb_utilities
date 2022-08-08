import sys
import os
import csv
from collections import OrderedDict
from collections import defaultdict
import json
import datetime
from decimal import Decimal
import random
import time
import re
import multiprocessing
import pprint
from deepmerge import Merger
import copy
import itertools
import shutil
import bson
from bson.objectid import ObjectId
from bson.json_util import dumps
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from faker import Faker
base_dir = os.path.dirname(os.path.abspath(__file__))
# apppend parent folder to path
sys.path.append(os.path.dirname(base_dir))
from bbutil import Util
from id_generator import Id_generator

sample_doc = "sample_doc.json"
fake = Faker()
categories = ["Running", "Cycling","CrossFit","OrangeTheory","Walking","Swimming","Jujitsu","SpeedKnitting"]

#------------------------------------------------------------------#
#     MAIN
#------------------------------------------------------------------#
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    doct_t = bb.read_json(sample_doc)
    CUR_PATH = os.path.dirname(os.path.realpath(__file__))
    ans = []
    bb.logit("START")
    for k in range(1000):
        cur_doc = bb.read_json(sample_doc) #copy.deepcopy(doct_t)
        cur_doc["data"][0]["category"] = random.choice(categories)
        ans.append(cur_doc)
    for k in ans:
        bb.logit(k["data"][0]["category"])
    bb.logit("END")
    