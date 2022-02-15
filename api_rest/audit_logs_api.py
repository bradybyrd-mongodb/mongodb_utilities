#!/usr/bin/env python3
#------------------------------------------------#
#  Audit Logs API - Fetch audit logs from Atlas API
#------------------------------------------------#

import sys
import os
from collections import OrderedDict
import json
import datetime as dt
import pathlib
import time
import re
import multiprocessing
import pprint
import bson
from bson.objectid import ObjectId
from bb_util import Util
import requests
from requests.auth import HTTPDigestAuth
#from pymongo import MongoClient

'''
#------------------------------------------#
  Notes -
  Call v1 rest api for Atlas
#
'''
settings_file = "audit_logs_api_settings.json"

def atlas_cluster_info(details = {}):
    result = {"nothing to show here": True}
    project_id = settings["project_id"]
    if "project_id" in details:
        project_id = details["project_id"]
    if "verbose" in ARGS:
        details["verbose"] = True
    if "name" in ARGS:
        cluster_name = ARGS["name"]
        url = f'{base_url}/groups/{project_id}/clusters/{cluster_name}'
    elif "name" in details:
        cluster_name = details["name"]
        url = f'{base_url}/groups/{project_id}/clusters/{cluster_name}'
    elif "all" in ARGS or "all" in details:
        url = f'{base_url}/clusters'
    else:
        url = f'{base_url}/groups/{project_id}/clusters'
    raw_result = rest_get(url, details) #, {"verbose" : True})
    if "results" in raw_result:
        result = raw_result["results"]
        if "clusters" in raw_result:
            result = result["clusters"]
    else:
        result = raw_result
    if not "quiet" in details:
        bb.message_box("Atlas Cluster Info", "title")
        pprint.pprint(raw_result)
    return result


def atlas_log_files(details = {}):
    '''
    Get log files every xx minutes and push to mongo
    '''
    cluster = settings["cluster_name"]
    log_dir = settings["log_path"]
    freq = settings["frequency"]
    end_time = dt.datetime.now()
    start_time = end_time - dt.timedelta(minutes=freq)
    path = pathlib.Path(log_dir)
    path.mkdir(parents=True, exist_ok=True)
    file_path = os.path.join(log_dir, "last_query.txt")
    if os.path.isfile(file_path):
        with open(file_path, 'r') as out_file:
          raw_time = out_file.read()
          raw_time = int(raw_time.replace("LastEnd=",""))
          start_time = dt.datetime.fromtimestamp(raw_time)

    bb.message_box(f'Fetching Atlas logs for {start_time.strftime("%m/%d/%Y")}, {cluster} between {start_time.strftime("%H:%M:%S")}-{end_time.strftime("%H:%M:%S")}', "title")
    path = pathlib.Path(log_dir)
    path.mkdir(parents=True, exist_ok=True)
    result = atlas_cluster_info({"name" : cluster, "quiet" : True})
    c_string = result["connectionStrings"]["standard"]
    members = c_string.split(",")
    for node in members:
        host = node.replace("mongodb://","")
        host = re.sub("\:27017.*","",host)
        get_log_file(host, start_time, end_time, log_dir, details)
    with open(file_path, 'wb') as out_file:
        cont = f'LastEnd={int(end_time.timestamp())}'
        out_file.write(cont.encode())


def get_log_file(cluster, start, end, log_path, details = {}):
    '''
    curl --user '{PUBLIC-KEY}:{PRIVATE-KEY}' --digest \
 --header 'Accept: application/gzip' \
 --request GET "https://cloud.mongodb.com/api/atlas/v1.0/groups/{GROUP-ID}/clusters/{HOSTNAME}/logs/mongodb.gz?startDate=&endDate=<unixepoch>" \
 --output "mongodb.gz
    '''
    details["headers"] = {"Content-Type" : "application/json", "Accept" : "application/gzip" }
    unixstart = dt.datetime(1970,1,1)
    #start_date = int((start - unixstart).total_seconds())
    #end_date = int((end - unixstart).total_seconds())
    start_date = int(start.timestamp())
    end_date = int(end.timestamp())
    fil = f'log_{cluster}_{end_date}.gz'
    file_path = os.path.join(log_path, fil)
    details["filename"] = file_path
    if not "quiet" in details:
        bb.logit(f'Node: {cluster}, Saving: {fil}')
    url = f'{base_url}/groups/{settings["project_id"]}/clusters/{cluster}/logs/mongodb.gz?startDate={start_date}&endDate={end_date}'
    result = rest_get_file(url, details)
    if not "quiet" in details:
        pprint.pprint(result)
    return result #result["results"]

def rest_get(url, details = {}):
  headers = {"Content-Type" : "application/json", "Accept" : "application/json" }
  if "headers" in details:
      headers = details["headers"]
  api_pair = api_key.split(":")
  response = requests.get(url, auth=HTTPDigestAuth(api_pair[0], api_pair[1]), headers=headers)
  result = response.content.decode('ascii')
  if "verbose" in details:
      bb.logit(f"Status: {response.status_code}")
      bb.logit(f"Headers: {response.headers}")
      bb.logit(f"URL: {url}")
      bb.logit(f"Response: {result}")
  return(json.loads(result))

def rest_get_file(url, details = {}):
  # https://stackoverflow.com/questions/36292437/requests-gzip-http-download-and-write-to-disk
  headers = {"Content-Type" : "application/json", "Accept" : "application/json" }
  if "headers" in details:
      headers = details["headers"]
  api_pair = api_key.split(":")
  local_filename = details["filename"]
  try:
      response = requests.get(url, auth=HTTPDigestAuth(api_pair[0], api_pair[1]), headers=headers, stream=True)
  except Exception as e:
      print(e)
  raw = response.raw
  with open(local_filename, 'wb') as out_file:
    cnt = 1
    while True:
        chunk = raw.read(1024, decode_content=True)
        if not chunk:
            break
        bb.logit(f'chunk-{cnt}')
        out_file.write(chunk)
        cnt += 1
  '''
  with requests.get(url, auth=HTTPDigestAuth(api_pair[0], api_pair[1]), headers=headers, stream=True) as r:

    r.raise_for_status()
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            # If you have chunk encoded response uncomment if
            # and set chunk_size parameter to None.
            #if chunk:
            f.write(chunk)
  '''
  if "verbose" in details:
      bb.logit(f"URL: {url}")
  return(local_filename)

def rest_post(url, details = {}):
  headers = {"Content-Type" : "application/json", "Accept" : "application/json"}
  if "headers" in details:
      headers = details["headers"]
  api_pair = api_key.split(":")
  post_data = details["data"]
  response = requests.post(url, auth=HTTPDigestAuth(api_pair[0], api_pair[1]), data=json.dumps(post_data), headers=headers)
  result = response.json() #content.decode('ascii')
  if "verbose" in details:
      bb.logit(f"Status: {response.status_code}")
      bb.logit(f"Headers: {response.headers}")
      bb.logit(f"Response: {json.dumps(result)}")
  return(result) #json.loads(result))

def rest_update(url, details = {}):
  headers = {"Content-Type" : "application/json", "Accept" : "application/json"}
  api_pair = api_key.split(":")
  post_data = details["data"]
  if isinstance(post_data, str):
      post_data = json.loads(post_data)
      print(post_data)
  response = requests.patch(url, auth=HTTPDigestAuth(api_pair[0], api_pair[1]), data=json.dumps(post_data), headers=headers)
  result = response.json() #content.decode('ascii')
  if "verbose" in details:
      bb.logit(f"Status: {response.status_code}")
      bb.logit(f"Headers: {response.headers}")
      bb.logit(f"Response: {json.dumps(result)}")
  return(result) #json.loads(result))

#------------------------------------------------------------------#
#     MAIN
#------------------------------------------------------------------#
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    api_key = f'{settings["api_public_key"]}:{settings["api_private_key"]}'
    base_path = os.path.dirname(os.path.abspath(__file__))

    base_url = settings["base_url"]
    if "action" not in ARGS:
        #print("Send action= argument")
        #sys.exit(1)
        atlas_log_files()
    elif ARGS["action"] == "cluster_info":
        atlas_cluster_info()
    elif ARGS["action"] == "logs":
        atlas_log_files()
    elif ARGS["action"] == "test":
        template_test()
    else:
        print(f'{ARGS["action"]} not found')
