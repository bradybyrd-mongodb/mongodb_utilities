#!/usr/bin/python3
import sys
import json
import datetime
import urllib
import os
import io
import subprocess
import time
import re
import multiprocessing
import pprint
from pymongo import MongoClient
from bson.objectid import ObjectId
base_dir = os.path.dirname(os.path.abspath(__file__))
# apppend parent folder to path
sys.path.append(os.path.dirname(base_dir))
from bbutil import Util
settings_file = "perf_test_settings.json"

#-----------------------------------------------------------#
#------------------------  Methods ----------------------------#
def test_oa():
    client = client_connection("oasource.uri")
    mdb = client[settings["oasource"]["database"]]
    coll = "tti_events"
    start = datetime.datetime.now()
    cnt = 0
    h_start = 0
    h_end = 6
    for k in range(4):
        tstart = datetime.datetime(2025, 12, 4, h_start, 0, tzinfo=datetime.timezone.utc)
        tend = datetime.datetime(2025, 12, 4, h_end, 0, tzinfo=datetime.timezone.utc)
        query = [
            {"$match": {
                "metadata.deviceId": {"$in": 
                ['4649344', '6035322', '5965887', '5710851', '3062597']},
            "$and": [
                {"timestamp": { "$gt": tstart}}, 
                {"timestamp": { "$lte": tend}}]
            }}, 
            {"$count": "total"}
        ]
        h_start = h_end
        h_end += 6
        #pprint.pprint(query)
        ans = mdb[coll].aggregate(query)
        for rec in ans:
            cnt = rec["total"]
            #print(f'Cnt: {cnt}')
        bb.timer(start, cnt)
        start = datetime.datetime.now()

# -----------------------------------------------------------#
# Deduping tests
all_deviceIds = [
    "5841979",
    "6468930",
    "3230305",
    "5160063",
    "3022457",
    "5161065",
    "3224461",
    "5270769",
    "5229182",
    "4731487",
    "5228498",
    "5183470",
    "4761581",
    "4483805",
    "5494594",
    "6265896",
    "5630082",
    "6333155",
    "6369288",
    "5183017",
    "5710897",
    "4701643",
    "6266190",
    "6011004",
    "3065233",
    "3228109",
    "5721447",
    "5749472",
    "4785555",
    "5998117",
    "5710616",
    "5229869",
    "3056353",
    "4650887",
    "6266198",
    "5227977",
    "4701161",
    "3228973",
    "5966497",
    "6146857",
    "5917556",
    "3035277",
    "5595545",
    "3029813",
    "6302631",
    "6554881",
    "5855527",
    "6399167",
    "6146977",
    "6034742",
    "4785180",
    "5619142",
    "6469056",
    "3051405",
    "3034049",
    "3231893",
    "5821262",
    "5564407",
    "4576135",
    "4976235",
    "6265811",
    "4482825",
    "5998259",
    "4559105",
    "5158990",
    "6114459",
    "5229350",
    "2708873",
    "3012225",
    "6147474",
    "6469951",
    "5075634",
    "4976511",
    "2710813",
    "4481233",
    "6543057",
    "5965496",
    "4809971",
    "6091592",
    "3028489",
    "5408026",
    "6035704",
    "3060917",
    "3056725",
    "5227056",
    "5753587",
    "5159573",
    "5966055",
    "5227264",
    "3062429",
    "5855459",
    "5753390",
    "5494491",
    "5916228",
    "4576331",
    "6114757",
    "3022201",
    "4559411",
    "6035032",
    "5022330"
]

more_device_ids = ["5494084",
"4649187",
"1858819",
"5631017",
"3016154",
"6181852",
"3018009",
"5965464",
"2746203",
"6266165",
"5537612",
"5310966",
"3064075",
"5158089",
"5076462",
"6399013",
"6065688",
"5271298",
"5564767",
"6266611",
"5710780",
"6147657",
"4576684",
"3026719",
"6115506",
"6543181",
"3012381",
"5159347",
"3050391",
"4975900",
"2706921",
"4768172",
"3230778",
"5732755",
"3036409",
"5877004",
"5917508",
"6035437",
"6052710",
"6052647",
"5457879",
"5630914",
"4576229",
"5630176",
"3231829",
"5710034",
"5158719",
"5182902",
"4480553",
"1760833",
"5707052",
"4482702",
"6099910",
"4558992",
"5159348",
"6034758",
"5158787",
"5594883",
"6469684",
"5825790",
"1897023",
"5394894",
"5227297",
"5157966",
"5763928",
"5310816",
"3015722",
"3019950",
"6554992",
"5537555",
"1901226",
"5494736",
"5887342",
"5719217",
"5789278",
"5763222",
"5763548",
"6237818",
"5158498",
"5966220",
"3031719",
"3057763",
"5966398",
"3034477",
"3027933",
"6011100",
"2705305",
"3029455",
"3033471",
"5227005",
"5227959",
"3232667",
"6265861",
"5916959",
"2708851",
"6211871",
"6065736",
"2707606",
"5763331",
"2705189",
"4496933",
"6555100",
"6099664",
"6237748",
"6091675",
"5721910",
"4496365",
"4976257",
"6147079",
"5270400",
"5494624",
"4497453",
"3230662",
"5408302",
"3055469",
"5183355",
"3040823",
"4654522",
"5595088",
"5979988",
"5979712",
"5458234",
"6927372",
"5710370",
"3052382",
"6181723",
"6554057",
"5967485",
"5785463",
"3231045",
"5271244",
"5966373",
"4760981",
"6092392",
"3063629",
"3231647",
"4558792",
"5979961",
"3066627",
"3046585",
"6181796",
"3018181",
"6115296",
"4650703",
"3065737",
"3232547",
"6326174",
"6091708",
"5158818",
"3060546",
"5902834",
"2604089",
"2744145",
"5785193",
"5710273",
"4502967",
"2705449",
"4654529",
"5965915",
"4480458",
"2820779",
"5537427",
"3012281",
"5270724",
"3056530",
"5485559",
"6266656",
"5375241",
"5966253",
"3016091",
"6398043",
"6182315",
"2604325",
"5965535",
"5967251",
"6114146",
"5595815",
"5604294",
"3066109",
"5967260",
"5485494",
"4481638",
"6035316",
"5270834",
"5183595",
"5537505",
"3048817",
"5537462",
"4809941",
"5395042",
"3019373",
"4650215",
"5916297",
"5841848",
"5362782",
"5631162",
"3059274",
"6035231",
"5917288",
"5903316",
"4496011",
"3231199",
"3055727",
"5271428",
"3046353",
"5564680",
"3023577",
"3037667",
"3067141",
"5917198",
"2744035",
"5159142",
"6035107",
"3057154",
"6099903",
"6791264",
"3224179",
"3026709",
"2743861",
"5630872",
"5135662",
"4760511",
"5916282",
"2742591",
"5494417",
"5764784",
"5969599",
"5537362",
"3036179",
"5457137",
"3012217",
"3060441",
"4761241",
"3052233",
"4576842",
"4483567",
"3018313",
"5233141",
"6927923",
"5706805",
"5088308",
"3051154",
"6469014",
"5394653",
"4700889",
"5721578",
"5855748",
"5821149",
"5136270",
"5965861",
"5158797",
"5630793",
"4650644",
"5604132",
"4559758",
"3054495",
"4576491",
"1841859",
"5630879",
"4761110"]

# Python dictionary representation of the aggregation pipeline
def dedup_pipeline(device_ids, start_date, end_date, dedup = True):
    dedup_pipeline = [
    {
        '$match': {
            'metadata.deviceId': {
                '$in': device_ids
            }, 
            'timestamp': {
                '$gt': start_date, 
                '$lte': end_date
            }
        }
    }, {
        '$project': {
            'metadata': 1, 
            'timestamp': 1, 
            'server_timestamp': 1, 
            'lynx_arrival_ts': 1, 
            'measurements': '$$ROOT'
        }
    }, {
        '$unset': [
            'measurements.metadata', 'measurements.timestamp', 'measurements.server_timestamp', 'measurements.lynx_arriva_ts'
        ]
    }, {
        '$sort': {
            'metadata.deviceId': 1, 
            'timestamp': 1, 
            'lynx_arrival_ts': -1
        }
    }, {
        '$group': {
            '_id': {
                'deviceId': '$metadata.deviceId', 
                'timestamp': '$timestamp'
            }, 
            'doc': {
                '$first': '$$ROOT'
            }
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$doc'
        }
    }, {
        '$sort': {
            'metadata.deviceId': 1, 
            'timestamp': -1
        }
    }, {
        '$group': {
            '_id': '$metadata.deviceId', 
            'records': {
                '$firstN': {
                    'input': '$$ROOT', 
                    'n': 100
                }
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'deviceId': '$_id', 
            'records': 1
        }
    }
]
    pipeline2 = [
        {
            '$match': {
                'metadata.deviceId': {
                    '$in': device_ids
                }, 
                'timestamp': {
                    '$gt': start_date, 
                    '$lte': end_date
                }
            }
        }, {
            '$sort': {
                'metadata.deviceId': 1, 
                'timestamp': -1
            }
        }
    ]
    return dedup_pipeline if dedup else pipeline2

def dedup_parallel():
    # python3 site_models.py action=load_data
    multiprocessing.set_start_method("fork", force=True)
    bb.message_box("Loading Data", "title")
    #bb.logit(f'# Settings from: {settings_file}')
    ids = more_device_ids
    num_procs = 4
    batch_size = int(len(ids)/num_procs) #25
    start_date = datetime.datetime(2025, 12, 17, 12, 38, 54)
    end_date = datetime.datetime(2025, 12, 18, 12, 38, 54)
    if "device_ids" in ARGS:
        ids = ARGS["device_ids"].split(",")
    if "start_date" in ARGS:
        start_date = datetime.datetime.fromisoformat(ARGS["start_date"])
    if "end_date" in ARGS:
        end_date = datetime.datetime.fromisoformat(ARGS["end_date"])
    dedup = True
    if "dedup" in ARGS:
        dedup = ARGS["dedup"].lower() == 'true'
    num_ids = len(ids)
    bb.logit(f'# Loading: {num_ids} devices from {num_procs} threads')
    jobs = []
    inc = 0
    cnt = 0
    start = datetime.datetime.now()
    for item in range(num_procs):
        device_ids = ids[cnt:cnt+batch_size]
        passed_args = {"start_date": start_date, "end_date": end_date, "device_ids": device_ids, "dedup": dedup}
    
        p = multiprocessing.Process(target=test_dedup, args = (item, passed_args))
        jobs.append(p)
        p.start()
        #time.sleep(1)
        inc += 1
        cnt += batch_size

    main_process = multiprocessing.current_process()
    for i in jobs:
        i.join()
    bb.timer(start, num_ids, "tot")
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    

def test_dedup(ipos, args):
    # python3 dedup_tester.py action=test_dedup device_ids="4649344,6035322,5965887,5710851,3062597" start_date=2025-12-17T12:38:54 end_date="2025-12-18T12:38:54" end_date="2025-12-18T12:38:54"
    batch_records = []
    verbose = False
    client = client_connection("livesource.uri")
    mdb = client[settings["livesource"]["database"]]
    coll = settings["livesource"]["collection"]
    start = datetime.datetime.now()
    cnt = 0
    maxids = 100
    if "maxids" in args:
        maxids = int(args["maxids"])
    substart = datetime.datetime.now()
    device_ids = args["device_ids"]
    num_ids = len(device_ids)
    if device_ids[0] == "all":
        device_ids = more_device_ids[0:maxids -1]
    if isinstance(args["start_date"], str):
        start_date = datetime.datetime.fromisoformat(args["start_date"])
        end_date = datetime.datetime.fromisoformat(args["end_date"])
    else:
        start_date = args["start_date"]
        end_date = args["end_date"]
    dedup = args["dedup"]
    if "verbose" in args:
        verbose = args["verbose"].lower() == 'true'
    query = dedup_pipeline(device_ids, start_date, end_date, dedup)        
    if verbose:
        pprint.pprint(query)  # Uncomment for debugging
    collection = mdb[coll]
    ans = collection.aggregate(query, allowDiskUse=True)
    bb.timer(substart, num_ids)
    if dedup:
        for rec in ans:
            batch_records.append(rec)
            print(f'Device: {rec["deviceId"]} - {len(rec["records"])} records')
    else:
        lastid = "zzzzzzz"
        icnt = 0
        for rec in ans:
            batch_records.append(rec)
            if rec["metadata"]["deviceId"] != lastid:
                print(f'Device: {lastid} - {icnt} records')
            
                icnt = 0
                lastid = rec["metadata"]["deviceId"]
            else:
                icnt += 1
        print(f'# Unique Devices: {icnt}')
    if verbose:
        print("# ----- Sample Record: ")
        pprint.pprint(batch_records[0])

#-----------------------------------------------------------#
#  Utility Code
#-----------------------------------------------------------#

def client_connection(type = "uri", details = {}):
    if "." in type:
        parts = type.split(".")
        mdb_conn = settings[parts[0]][parts[1]]
        username = settings[parts[0]]["username"]
        password = settings[parts[0]]["password"]
    else:
        mdb_conn = settings[type]
        username = settings["username"]
        password = settings["password"]
        if "username" in details:
            username = details["username"]
            password = details["password"]
    if "secret" in password:
        password = os.environ.get("_PWD_")
    if "%" not in password:
        password = urllib.parse.quote_plus(password)
    mdb_conn = mdb_conn.replace("//", f'//{username}:{password}@')
    bb.logit(f'Connecting: {mdb_conn}')
    if "readPreference" in details:
        client = MongoClient(mdb_conn, readPreference=details["readPreference"]) #&w=majority
    else:
        client = MongoClient(mdb_conn)
    return client

#-----------------------------------------------------------#
#------------------------  MAIN ----------------------------#
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    base_counter = settings["base_counter"]
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "oa_test":
        test_oa()
    elif ARGS["action"] == "dedup":
        dedup_parallel()
    elif ARGS["action"] == "test_dedup":
        if "device_ids" in ARGS:
            ARGS["device_ids"] = ARGS["device_ids"].split(",")
        test_dedup(0,ARGS)
    else:
        bb.logit(f'ERROR: {ARGS["action"]} not recognized')
