#!/usr/bin/env python

'''
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!   NOTE: SCRIPT NEEDS TO BE COMPATIBLE WITH PYPY3!
!   THIS SAMPLE IS BUILT USING MIMESIS 11.1.0.
!   IF YOU ARE USING A SCRIPT THAT USES AN OLDER VERSION,
!   YOU NEED TO EITHER UPGRADE YOUR CODE TO MATCH THIS TEMPLATE
!   OR GO INTO THE REQUIREMENTS FILE AND CHANGE THE MIMESIS VERSION
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
'''

########################################################################
# 
# This is an example Locust file that use Mimesis to help generate
# dynamic documents. Mimesis is more performant than Faker
# and is the recommended solution. After you build out your tasks,
# you need to test your file in mLocust to confirm how many
# users each worker can support, e.g. confirm that the worker's CPU
# doesn't exceed 90%. Once you figure out the user/worker ratio,
# you should be able to figure out how many total workers you'll need
# to satisfy your performance requirements.
#
# These Mimesis locust files can be multi-use, 
# saturating a database with data or demonstrating standard workloads.
#
########################################################################

# Docs: https://docs.google.com/document/d/1rxxjxZgfol7Pq0wpkMiotaCeNnyMUpZkJSmndcBPM2s/edit?tab=t.0
# MLocust: https://mlocust.mside.app/project-config

# Allows us to make many pymongo requests in parallel to overcome the single threaded problem
import gevent
from gevent import monkey
_ = gevent.monkey.patch_all()

########################################################################
# TODO Add any additional imports here.
# TODO But make sure to include in requirements.txt
########################################################################
import pymongo
from bson import json_util
from bson.json_util import loads
from bson import ObjectId
from bson.decimal128 import Decimal128
from locust import User, events, task, constant, tag, between, runners
import time
import traceback
import math
from pickle import TRUE
from datetime import datetime, timedelta
import random
from decimal import Decimal
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
from pymongo import UpdateMany
import pprint

# Global vars
# We can use this var to track the seq index of the worker in case we want to use it for generating unique seq keys in mimesis
_WORKER_ID = "T-1"
# Store the client conn globally so we don't create a conn pool for every user
# Track the srv globally so we know if we need to reinit the client
_CLIENT = None
_SRV = None
# Track the full host path so we know if anything changes
_HOST = None
_VERSION = None
#fake = Faker()

# docs to insert per batch insert
_SETTINGS = {
    "uri": "mongodb+srv://main_admin:<secret>@iot-ingest.p3wh3.mongodb.net",
    "uri_check": "mongodb+srv://main_admin:<secret>@claims-demo.vmwqj.mongodb.net",
    "database": "building_monitor",
    "collection": "sh_readings",
    "base_id" : 1000000,
    "count_coll" : "counts",
    "batch_size": 500,
    "username": "main_admin",
    "password": "<secret>",
    "version" : "3.1",
    "devices" : 40000,
    "pollers" : 10,
    "users" : 5,
    "gate" : "off",
    "device_types" : {
        1000 : {"type" : "chiller", "avg" : 55, "unit" : "degrees"},
        2000 : {"type" : "air_handler", "avg" : 230, "unit" : "cfm"},
        3000 : {"type" : "boiler", "avg" : 180, "unit" : "degrees"},
        4000 : {"type" : "vav", "avg" : 85, "unit" : "degrees"},
        5000 : {"type" : "air_handler", "avg" : 35, "unit" : "bars"},
        6000 : {"type" : "fan", "avg" : 2600, "unit" : "rpm"},
        7000 : {"type" : "room_temp", "avg" : 65, "unit" : "degrees"},
        8000 : {"type" : "set_point", "avg" : 90, "unit" : "percent"},
        9000 : {"type" : "power", "avg" : 40, "unit" : "amps"},
        10000 : {"type" : "distribution", "avg" : 400, "unit" : "amps"},
        11000 : {"type" : "backup_generator", "avg" : 55, "unit" : "amps"},
        12000 : {"type" : "air_pressure", "avg" : 230, "unit" : "cfm"},
        13000 : {"type" : "solar_panel", "avg" : 180, "unit" : "voltage"},
        14000 : {"type" : "solar_regulator", "avg" : 85, "unit" : "efficiency"},
        15000 : {"type" : "back_pressure", "avg" : 35, "unit" : "bars"},
        16000 : {"type" : "water_pump", "avg" : 2600, "unit" : "rpm"},
        17000 : {"type" : "water_pressure", "avg" : 65, "unit" : "psi"},
        18000 : {"type" : "battery_status", "avg" : 90, "unit" : "percent"},
        19000 : {"type" : "heat_calling", "avg" : 100, "unit" : "rooms"},
        20000 : {"type" : "cooling_calling", "avg" : 100, "unit" : "rooms"}
    },
    "locations" : [[-73.9697795,40.7519846,"0"],
        [-73.9961458,40.7186459,"217"],
        [-73.9589499,40.8159927,"600"],
        [-73.9883734,40.7581681,"258"],
        [-73.8052915,40.7211216,"162-16"],
        [-73.932667,40.70524899999999,"12"],
        [-73.9845346,40.7015745,"155"],
        [-74.0007959,40.729197,"183"],
        [-73.999087,40.645929,"4118"],
        [-73.98964099999999,40.664741,"577"],
        [-73.9802564,40.7518458,"290"],
        [-73.989121,40.7362872,"42"],
        [-74.008558,40.732037,"110"],
        [-73.9488166,40.6509905,"3021"],
        [-73.9657703,40.7622135,"1011"],
        [-73.80217999999999,40.707304,"87-77"],
        [-73.850359,40.903902,"4711"],
        [-73.853285,40.74047,"55-27"],
        [-74.13513720000002,40.6252815,"1376"],
        [-73.9614666,40.59635,"2486"],
        [-74.00796,40.737398,"765"],
        [-73.990697,40.7269016,"63"],
        [-74.00974819999999,40.7237182,"458"],
        [-74.0096509,40.7237676,"460"],
        [-73.90681339999999,40.7456038,"5716"],
        [-73.7948663,40.7727025,"26-15"],
        [-73.8326848,40.7525152,"4444"],
        [-73.9058659,40.7456127,"5811"],
        [-73.947964,40.775257,"1607"],
        [-73.9859414,40.6986772,"0"],
        [-73.9637815,40.8034893,"1028"],
        [-73.88228649999999,40.6481972,"1366"],
        [-73.9596167,40.7200363,"125"],
        [-74.0032228,40.74352150000001,"130"],
        [-73.994648,40.722672,"24"],
        [-73.817238,40.751569,"141-20"],
        [-73.984111,40.691241,"229"],
        [-73.933092,40.849632,"1430"],
        [-73.9607843,40.5895705,"2744"],
        [-73.98452069999999,40.6707486,"346"],
        [-73.8554995,40.8552261,"2059"],
        [-73.8707599,40.7514596,"37-01"],
        [-73.9128443,40.8436636,"105"],
        [-73.9417793,40.6718814,"235"],
        [-73.9925306,40.7309346,"759"],
        [-73.9806361,40.7635061,"151"],
        [-73.987854,40.5864104,"2807"],
        [-73.9505712,40.6669964,"869"],
        [-73.9566231,40.8022119,"2082"],
        [-73.9308932,40.6167337,"2178"],
        [-73.95591399999999,40.7662842,"411"],
        [-73.81471359999999,40.75541380000001,"14714"],
        [-73.9558709,40.7250737,"202"],
        [-74.0147978,40.7151304,"225"],
        [-73.9447083,40.8137377,"2253"],
        [-73.9056678,40.7066898,"657"],
        [-73.91567599999999,40.757126,"44-08"],
        [-73.9885901,40.7349576,"119"],
        [-73.97691499999999,40.757028,"17"],
        [-73.9898074,40.71971509999999,"127"],
        [-73.99740589999999,40.7197962,"163"],
        [-73.9190555,40.7414223,"47-54"],
        [-73.8638838,40.7281002,"63-55"],
        [-74.17681800000001,40.601669,"3555"],
        [-73.8789566,40.7480319,"86-28"],
        [-73.9818586,40.7541476,"11"],
        [-73.9565561,40.5986529,"1422"],
        [-73.9976111,40.7223394,"126132"],
        [-73.9220296,40.6762625,"372"],
        [-73.9829694,40.7384858,"297"],
        [-73.93599499999999,40.697721,"921"],
        [-73.93113249999999,40.66642420000001,"337"],
        [-73.988428,40.73885,"43"],
        [-73.734503,40.772264,"25505"],
        [-73.99076149999999,40.7653444,"465"],
        [-73.8933073,40.754667,"73-18"],
        [-73.9265691,40.8288189,"1"],
        [-73.9961364,40.71397,"27"],
        [-73.98361489999999,40.7527864,"36"],
        [-73.99837,40.716313,"69"],
        [-73.9836013,40.7642999,"250"],
        [-73.9098999,40.694258,"717"],
        [-73.99026099999999,40.728505,"41"],
        [-73.8194414,40.75055529999999,"45-72"],
        [-74.126328,40.612596,"1872"],
        [-73.81533,40.762775,"40-20"],
        [-73.987369,40.7209078,"115"],
        [-74.00682599999999,40.7428114,"425"],
        [-73.92861500000001,40.855985,"1636"],
        [-73.9653177,40.7167261,"229"],
        [-73.94937759999999,40.6486704,"1532"],
        [-73.93032130000002,40.8193837,"610"],
        [-73.98823060000001,40.7587649,"700"],
        [-73.8966456,40.70627169999999,"66-91"],
        [-74.0762324,40.6272621,"596"],
        [-73.924184,40.68904,"36 A"],
        [-74.00153,40.729076,"190"],
        [-74.08306739999999,40.604816,"2071"],
        [-73.9875035,40.72357179999999,"103"],
        [-73.8650105,40.6914788,"76-14"],
        [-73.98179259999999,40.7544806,"20"],
        [-74.00253959999999,40.7579403,"655"],
        [-74.0753665,40.6254224,"12"],
        [-73.7822056,40.6434612,"0"],
        [-74.0004998,40.7316042,"149"],
        [-73.98224689999999,40.6744051,"276"],
        [-73.9742747,40.7642202,"1"],
        [-73.91863839999999,40.7653613,"34-14"],
        [-73.9458667,40.7509105,"13-15"],
        [-73.990495,40.725212,"32"],
        [-73.9866285,40.7415086,"11"],
        [-73.9171281,40.7646773,"3610"],
        [-73.957817,40.729677,"109"],
        [-73.87575,40.8294396,"1599"],
        [-73.9149801,40.8300011,"1064"],
        [-73.9272984,40.8338973,"80"],
        [-73.977874,40.78422399999999,"426"],
        [-73.9138725,40.8237107,"860"],
        [-73.9466958,40.5840221,"2027"],
        [-74.1055512,40.6179186,"1150"],
        [-74.0002514,40.7273702,"100"],
        [-73.9176179,40.7462597,"4610"],
        [-73.77813909999999,40.6413111,""],
        [-74.000254,40.7172727,"148"],
        [-73.95277,40.76762799999999,"1374"],
        [-73.98622499999999,40.7613051,"250"],
        [-73.98681839999999,40.7619137,"301"],
        [-73.88235499999999,40.844641,"906"],
        [-74.0066131,40.737409,"328"],
        [-73.9801861,40.6872992,"372"],
        [-73.9942386,40.7521985,"461"],
        [-73.9511668,40.7258355,"105"],
        [-73.9810495,40.7539609,"500"],
        [-73.98818299999999,40.688511,"311"],
        [-73.9624161,40.7131783,"328"],
        [-73.98925299999999,40.712947,"213"],
        [-73.933087,40.6960679,"1011"],
        [-73.9046822,40.7455812,"5917"],
        [-73.99136870000001,40.6923048,"52"],
        [-73.984883,40.7235745,"153"],
        [-73.813163,40.786781,"15-07"],
        [-73.9102458,40.8856382,"564"],
        [-73.98853729999999,40.7697239,"885"],
        [-74.002511,40.73251399999999,"15"],
        [-73.9407308,40.808995,"2"],
        [-73.97693199999999,40.672901,"140"],
        [-73.9565666,40.6751066,"605"],
        [-73.999635,40.618124,"7206"],
        [-74.011352,40.633347,"6301"],
        [-73.9928876,40.6892494,"140"],
        [-73.9984186,40.73292560000001,"40"],
        [-73.9378765,40.8382073,"2127"],
        [-73.983909,40.758345,"147"],
        [-73.9763299,40.764312,"45"],
        [-73.94967919999999,40.7772719,"355"],
        [-73.8545481,40.7421626,"5316"],
        [-73.9091097,40.707017,"551"],
        [-73.74411649999999,40.6065258,"728"],
        [-73.788088,40.7578592,"19318"],
        [-73.84863700000001,40.710303,"104-05"],
        [-73.933026,40.75307400000001,"38-34"],
        [-73.822915,40.686426,"120-06"],
        [-73.94976299999999,40.682025,"95"],
        [-73.887976,40.853711,"613"],
        [-74.0096302,40.7208395,"57"],
        [-74.134152,40.632998,"330"],
        [-73.9883119,40.755487,"208"],
        [-73.9422079,40.756022,"12-09"],
        [-73.8474284,40.7818022,"120-07"],
        [-73.9107556,40.6923886,"1240"],
        [-73.91848399999999,40.64053699999999,"8002"],
        [-73.8506474,40.6796404,"87-20"],
        [-73.8668331,40.6786038,"1146"],
        [-73.862937,40.7301054,"95-56"],
        [-73.9560817,40.8041649,"2131"],
        [-74.001924,40.7346,"163"],
        [-73.84988249999999,40.73422840000001,"6327"],
        [-73.982942,40.7636829,"1695"],
        [-73.9745971,40.7501862,"639"],
        [-73.825867,40.67752,"112-15"],
        [-73.99426609999999,40.7508707,"4"],
        [-73.9416849,40.822537,"274"],
        [-74.1695711,40.577103,"2865"],
        [-73.9400488,40.7942388,"2171"],
        [-73.94703419999999,40.7002584,"657"],
        [-73.881249,40.756259,"86-09"],
        [-73.916893,40.758073,"42-13"],
        [-73.9528705,40.7717378,"1498"],
        [-74.1038564,40.5767978,"2211"],
        [-73.997697,40.71516440000001,"53"],
        [-73.9438279,40.8101851,"373"],
        [-73.96738189999999,40.792522,"731"],
        [-73.88995659999999,40.8099917,"500"],
        [-73.9864408,40.6917943,"146"],
        [-73.97523029999999,40.6831182,"620"],
        [-73.9485579,40.6437057,"1727"],
        [-73.9615456,40.7639097,"1228"],
        [-73.9736995,40.7507837,"205"],
        [-73.97271479999999,40.763118,"16"],
        [-73.97271479999777,40.763118,"146"],
        [-74.1110561,40.5884772,"900"]
    ]
}
@events.init.add_listener
def on_locust_init(environment, **_kwargs):
    global _WORKER_ID
    if not isinstance(environment.runner, runners.MasterRunner):
        _WORKER_ID = environment.runner.worker_index

class MetricsLocust(User):
    ########################################################################
    # Class variables. 
    # The values are initialized with None
    # till they get set from the actual locust exeuction 
    # when the host param is passed in.
    # DO NOT HARDCODE VARS! PASS THEM IN VIA HOST PARAM.
    # TODO Do you have more than 20 tasks? If so, change the array init below.
    ########################################################################
    client, db, coll, bulk_size = None, None, None, None

    def __init__(self, parent):
        global _, _WORKER_ID, _HOST, _CLIENT, _SRV, _VERSION, _SETTINGS

        super().__init__(parent)

        try:
            database = _SETTINGS["database"] 
            collection = _SETTINGS["collection"]
            srv = _SETTINGS["uri"]
            batch_size = _SETTINGS["batch_size"]
            if _VERSION is None:
                _VERSION = _SETTINGS["version"]
            isInit = (_HOST != self.host)
            
            # Singleton
            if isInit:
                print("Initializing...")
                # Parse out env variables from the host
                vars = self.host.split("|")
                srv = vars[0]
                print("SRV:",srv)
                database = vars[1]
                collection = vars[2]
                # docs to insert per batch insert
                batch_size = int(vars[3])
                if len(vars) > 4:
                    _VERSION = vars[4]
                if len(vars) > 5:
                   _SETTINGS["pollers"] = int(vars[5])
                if len(vars) > 6:
                    _SETTINGS["users"] = int(vars[6])
                if len(vars) > 7:
                    _SETTINGS["gate"] = vars[7]
                self.client = pymongo.MongoClient(srv)
                _CLIENT = self.client
                _HOST = self.host           
            else:
                # standalone operation (testing)
                self.client = pymongo.MongoClient(srv)
                _HOST = srv
            _SRV = srv
            self.db = self.client[database]
            self.coll = self.db[collection]
            _SETTINGS["batch_size"] = batch_size
            # docs to insert per batch insert
            self.bulk_size = batch_size
            print("Batch size from Host:",self.bulk_size)
            srv_check = _SETTINGS["uri_check"]
            _SETTINGS["lclient"] = None #pymongo.MongoClient(srv_check)
        
            print("SRV:",srv)
            self.audit("init", f'Starting Params: C:{database}.{collection}, B:{batch_size}, V:{_VERSION}')

            # init schema once (mimesys here)
            if isInit:
                boo = "boo"
        except Exception as e:
            # If an exception is caught, Locust will show a task with the error msg in the UI for ease
            events.request.fire(request_type="Host Init Failure", name=str(e), response_time=0, response_length=0, exception=e)
            raise e
        ################################################################

    # Audit should only be intended for logging errors
    # Otherwise, it impacts the load on your cluster since it's
    # extra work that needs to be performed on your cluster
    ################################################################
    def audit(self, type, msg, xtra = {}):
        global _VERSION
        print("Audit: ", msg)
        payload = {"type":type, "ts":datetime.now(), "version": _VERSION, "msg":str(msg)}
        if xtra != {}:
            for k in xtra:
                payload[k] = xtra[k]
        self.db.audit.insert_one(payload)

    ################################################################
    # Example helper function that is not a Locust task.
    # All Locust tasks require the @task annotation
    ################################################################
    def get_time(self):
        return time.time()

    ################################################################
    # We need to simulate polling result
    # this method will create polling response
    # with mcnt data points in the array
    ################################################################
    def generate_result(self, id_cnt, device_details, mcnt = 720):
        '''
            Scenario - rooftop has 500 devices
            each device reports per minute
            devices have a 10% range of operation in differing absolute amount
        '''
        dps = []
        cur = datetime.now()
        start = cur - timedelta(hours=24)
        rlow = device_details["avg"] * 10
        rhigh = int(device_details["avg"] * 10 * 1.1)
        for k in range(mcnt):
            dps.append(random.randint(rlow,rhigh)/10)
        minval = min(dps)
        minpos = dps.index(minval)
        mintime = start + timedelta(minutes=minpos)
        maxval = max(dps)
        maxpos = dps.index(maxval)
        maxtime = start + timedelta(minutes=maxpos)
        doc = {
            "measurement_id": id_cnt,
            "bldg": device_details["building"],
            "location": device_details["location"],
            "type": device_details["type"],
            "unit": device_details["unit"],
            "deviceDataID": device_details["id"],
            "date": datetime.now(),
            "dataPoints": dps,
            "pointCount": mcnt,
            "pointMax": maxpos,
            "pointMin": minpos,
            "minValue": minval,
            "minDateTime": mintime,
            "maxValue": maxval,
            "maxDateTime": maxtime,
            "lastPointDateTime": cur,
            "lastPointValue": dps[mcnt - 1],
            "version": _VERSION
        }
        return doc

    ################################################################
    # We need to simulate polling update
    # this method will update
    #  
    ################################################################
    def generate_measurement(self, cur_item):
        '''
            Generate updates to add to measurement array
        '''
        dps = []
        cur = datetime.now()
        for k in range(5):
            dps.append(cur_item["meas"] * (random.randint(8,12)/10))
        pipe = {"$addToSet" : {"dataPoints" : {"$each" : dps}},"$inc" : {"pointCount" : 5}, "$set" : { "lastPointValue" : dps[-1], "lastPointDateTime" : cur}}
        return pipe
    
    def generate_measurement_insert(self, cur_id):
        '''
            Generate updates to add to measurement array
        '''
        global version
        cur = datetime.now()
        device_id = random.randint(1000,9999)
        device_details = self.device_type(device_id)
        rlow = device_details["avg"] * 10
        rhigh = int(device_details["avg"] * 10 * 1.1)
        cur_val = device_details["avg"] + ((random.randint(1,100)/100) * device_details["avg"])
        doc = {
            "measurement_id" : cur_id,
            "ts" : cur,
            "cur_value" : cur_val,
            "version" : version
        }
        return doc

    def bulk_writer(self,collection, bulk_arr, msg = ""):
        try:
            result = collection.bulk_write(bulk_arr, ordered=False)
            ## result = db.test.bulk_write(bulkArr, ordered=False)
            # Opt for above if you want to proceed on all dictionaries to be updated, even though an error occured in between for one dict
            #pprint.pprint(result.bulk_api_result)
            note = f'BulkWrite - mod: {result.bulk_api_result["nModified"]} {msg}'
            #file_log(note,locker,hfile)
            print(note)
        except BulkWriteError as bwe:
            print("An exception occurred ::", bwe.details)

    def id_gen(self, icnt):
        # Get a bullk amount of ids
        idcoll = self.db[_SETTINGS["count_coll"]]
        ans = idcoll.find_one_and_update({ "_id": "UNIQUE COUNT DOCUMENT IDENTIFIER" },
            {"$inc": {"counter": icnt }})
        return ans["counter"]
    
    def next_measurement(self, deviceid):
        global _SETTINGS
        avg = _SETTINGS["device_types"][math.floor(deviceid/1000) * 1000]["avg"]
        return random.randint(int(avg * 9),int(avg * 11))/10
    
    def id_sampler(self, siz):
        # take 40k random items and store info
        pipe = [
            {"$sample" : {"size" : siz}},
            {"$project" : {"measurement_id": 1, "deviceDataID": 1, "_id": 0}}
        ]
        result = list(self.coll.aggregate(pipe))
        cnt = 0
        for k in result:
            result[cnt]["meas"] = self.next_measurement(k["deviceDataID"])
            cnt += 1
        return result
    
    def generate_devices(self):
        global _SETTINGS
        devices = []
        locations = _SETTINGS["locations"]
        for k in range(20000):
            device_id = random.randint(1000,20999)
            bldg = locations[random.randint(0,200)]
            device_details = _SETTINGS["device_types"][math.floor(device_id/1000) * 1000]
            doc = {"id": k, "type": device_details["type"], "avg": device_details["avg"], "unit": device_details["unit"], "location": [bldg[0],bldg[1]], "building": bldg[2]}
            devices.append(doc)
        return(devices)
       
    ################################################################
    # Since the loader is designed to be single threaded with 1 user
    # There's no need to set a weight to the task.
    # Do not create additional tasks in conjunction with the loader
    # If you are testing running queries while the loader is running
    # deploy 2 clusters in mLocust with one running faker and the
    # other running query tasks
    # The reason why we don't want to do both loads and queries is
    # because of the simultaneous users and wait time between
    # requests. The bulk inserts can take longer than 1s possibly
    # which will cause the workers to fall behind.
    ################################################################
    @task(100)
    def _bulkinsert(self):
        global _VERSION, _SETTINGS, _WORKER_ID 
        print(f'Inserting: B-{self.bulk_size}, stuff')
        # Note that you don't pass in self despite the signature above
        tic = self.get_time()
        name = "bulkInsert"
        batch_size = self.bulk_size
        try:
            arr = []
            cur_id = self.id_gen(batch_size)
            devices = self.generate_devices()
            for _ in range(batch_size):
                device_id = random.randint(0,19999)
                measurement_id = f"M-{cur_id}"
                details = devices[device_id]
                arr.append(self.generate_result(measurement_id, details, 5))
                cur_id += 1
            
            self.coll.insert_many(arr, ordered=False)
            self.audit("tally", f"[{_WORKER_ID}] bulk_insert - {batch_size} - response_time={'{:.3f}'.format((time.time()-tic)*1000)}", {"tally" : batch_size} )
            arr = []
            events.request.fire(request_type="mlocust", name=name, response_time=(self.get_time()-tic)*1000, response_length=0)
        except Exception as e:
            print(f'Exception: {e}')
            print(traceback.format_exc())
            events.request.fire(request_type="mlocust", name=name, response_time=(self.get_time()-tic)*1000, response_length=0, exception=e)
            # Add a sleep so we don't overload the system with exceptions
            time.sleep(5)

    @task(1)
    def _bulkupdate(self):
        # Note that you don't pass in self despite the signature above
        '''
            Ideally, do this every 30 seconds forever as a single execution
            so, if extra users represent the same poller, divide by that
        '''
        global _VERSION, _SETTINGS, _WORKER_ID
        # Force to execute every 30 seconds using external gate
        poll_time = int(30/_SETTINGS["users"])
        tic = self.get_time()
        name = "bulkupdate"
        batch_size = self.bulk_size
        batches = int(_SETTINGS["devices"]/(batch_size * _SETTINGS["users"]))
        try:
            update_arr = []
            # Now perform updates agains any value
            for it in range(batches):
                print(f'Performing batch {it}')
                subtic = self.get_time()
                source_data = self.id_sampler(batch_size)
                for item in source_data:
                    clause = self.generate_measurement(item)
                    update_arr.append(UpdateOne({"measurement_id": item["measurement_id"]}, clause))
                self.bulk_writer(self.coll, update_arr)
                self.audit("tally", f"[{_WORKER_ID}] bulk_update - {batch_size}-{it} - response_time={'{:.3f}'.format((time.time()-subtic)*1000)}", {"tally" : batch_size} )
                pprint.pprint(update_arr)
                update_arr = []
            events.request.fire(request_type="mlocust", name=name, response_time=(time.time()-tic)*1000, response_length=0)
            self.audit("success", f"[{_WORKER_ID}] bulk_update-done - {batch_size * batches} - response_time={'{:.3f}'.format((time.time()-tic)*1000)}" )
            if _SETTINGS["gate"] == "on" and interval > 0:
                interval = int(poll_time - (time.time() - tic))
                time.sleep(interval)
        except Exception as e:
            #events.request_failure.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            events.request.fire(request_type="mlocust", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            self.audit("exception", e)
            # Add a sleep for just faker gen so we don't hammer the system with file not found ex
            #time.sleep(5)