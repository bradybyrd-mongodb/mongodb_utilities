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
import string
import getopt
import bson
from bson.objectid import ObjectId
from bson.json_util import dumps
from bbutil import Util
from id_generator import Id_generator
from pymongo import MongoClient
from google.cloud import spanner, spanner_admin_database_v1
from google.cloud.spanner_admin_database_v1.types.common import DatabaseDialect
from google.cloud.spanner_v1 import param_types
from google.cloud.spanner_v1.data_types import JsonObject
from google.cloud.spanner_admin_database_v1.types import spanner_database_admin

database_id= "healthcare"
instance_id = "bb-spanner-poc"

spanner_client = spanner.Client()
instance = spanner_client.instance(instance_id)
database = instance.database(database_id)

fields = ('quote_id',
 'session_id',
 'cart_id',
 'name',
 'email',
 'createddate',
 'effectivedate',
 'expirationdate',
 'statuscode',
 'version',
 'premium',
 'isactive',
 'modified_at')

vals = [
    ('P-2030000',
    'PR-2000092',
    'CA-2000020',
    'John Villanueva',
    'michael56@example.org',
    '2023-08-22',
    '2024-06-03',
    '2024-06-17',
    '30',
    '1.0',
    183,
    'true',
    '2024-06-17 16:13:24'),
    ('P-2030001',
    'PR-2000064',
    'CA-2000083',
    'Kelly Robinson',
    'johnskinner@example.com',
    '2023-08-22',
    '2024-06-03',
    '2024-06-17',
    '30',
    '1.0',
    733,
    'true',
    '2024-06-17 16:13:24'),
    ('P-2030002',
    'PR-2000074',
    'CA-2000027',
    'Jennifer Miller',
    'kramerlori@example.com',
    '2023-08-22',
    '2024-06-07',
    '2024-06-17',
    '40',
    '1.0',
    414,
    'true',
    '2024-06-17 16:13:24') 
]

vals = [
    ('P-20300244',
    'PR-2000092',
    'CA-2000020',
    'John Villanueva',
    'michael56@example.org',
    '2023-08-22',
    '2024-06-03',
    '2024-06-17',
    '30',
    '1.0',
    183,
    'true',
    datetime.datetime.now())
]
with database.batch() as batch:
    batch.insert(
        table="quote",
        columns=fields,
        values=vals,
    )

print("Inserted data.")

