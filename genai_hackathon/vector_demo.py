import os
import sys
# apppend parent folder to path
base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(base_dir))
import requests
import datetime
from io import BytesIO
from bs4 import BeautifulSoup
import csv
import pprint
import json
import urllib.parse
from pymongo import MongoClient
from bbutil import Util
import nltk
nltk.download("punkt")  # Download the necessary data for tokenization


def vector_query():
    # https://www.mongodb.com/docs/atlas/atlas-search/query-syntax/
    start_time = datetime.datetime.now()
    conn = mongodb_connection()
    database = settings["database"]
    collection = settings["collection"]
    db = conn[database]
    vector_index = "default"
    text_index = "full_text"
    prompt = "Lovely day!"
    num_results = 4
    dedup = False
    full_sentence = False
    if "prompt" in ARGS:
        prompt = ARGS["prompt"]
    else:
        bb.logit("ERROR: enter a prompt= parameter")
        sys.exit(1)
    if "num" in ARGS:
        num_results = int(ARGS["num"])
    prompt_vector = get_vector(prompt)
    #pprint.pprint(prompt_vector)
    pipe = [
        {
        "$vectorSearch": {
            "index": vector_index,
            "queryVector": prompt_vector,
            "path": "sentence_vec",
            "limit": num_results,
            "numCandidates": 5}
        },
        {"$project": {
            "_id": 0,
            "plan": 1,
            "url" : 1,
            "lang" : 1,
            "sentence": 1,
            "score": { "$meta": "vectorSearchScore" }}
        }
    ]
    extra_args(pipe)
    #pprint.pprint(pipe)
    result = db[collection].aggregate(pipe)
    bb.message_box("Search Results","title")
    bb.logit(f'Searching: {prompt}')
    for doc in result:
        pprint.pprint(doc)
        print(f'# --------- Score: {doc["score"]} --------- #')
        print(f'Sentence: {doc["sentence"]}')
        print(f'Source: {doc["url"]}')

def hybrid_query():
    vector_scalar = 0.9 
    vector_normalization = 40 
    fts_scalar = 1 - vector_scalar 
    fts_normalization = 10
    num_results = 5
    start_time = datetime.datetime.now()
    if "prompt" in ARGS:
        prompt = ARGS["prompt"]
    else:
        bb.logit("ERROR: enter a prompt= parameter")
        sys.exit(1)
    conn = mongodb_connection()
    database = settings["database"]
    collection = settings["collection"]
    db = conn[database]
    vector_index = "default"
    text_index = "full_text"
    if "num" in ARGS:
        num_results = int(ARGS["num"])
    prompt_vector = get_vector(prompt)
    filter = ""
    num_results = 4
    dedup = False
    #  Vector Search
    vector_stage = {
            "$vectorSearch": {
              "index": vector_index,
              "queryVector": prompt_vector,
              "path": "sentence_vec",
              "limit": num_results,
              "numCandidates": 5}
          }
    #  Text-based search    
    search_pipe = [
        {'$search': {
            'index': text_index, 
            'text': {
              'query': prompt, 
              'path': 'sentence'
            },

          }
        },
        {
            "$limit" : num_results
        },
        {
          "$unset":['sentence_vec']
        },
        {
          '$addFields':{
            "source":"textSearch",
            'metascore': {
              '$meta': 'searchScore'
            }
          }
        },
        {
          '$set': {
            "score": {"$multiply": ["$metascore", fts_scalar / fts_normalization]},
          }
        }
    ]
    # Now collect it into a single pipe for both search methods
    pipe = [
        vector_stage,
        {
          "$unset":['sentence_vec']
        },
        {
          '$addFields':{
            "source":"vectorSearch",
            'metascore': {
              '$meta': 'vectorSearchScore'
            }
          }
        },
        {
          '$set': {
            "score": {"$multiply": ["$metascore", vector_scalar / vector_normalization]},
          }
        },
        {
            "$unionWith" : {
                "coll" : collection,
                "pipeline" : search_pipe}
        }
    ]

    extra_args(pipe)    
        #pprint.pprint(pipe)
    result = db[collection].aggregate(pipe)
    bb.message_box("Hybrid Search Results","title")
    bb.logit(f'Searching: {prompt}')
    for doc in result:
        print(f'# --------- Score: {doc["score"]} --------- #')
        print(f'Sentence: {doc["sentence"]}')
        print(f'Source: {doc["url"]}, src-{doc["source"]}')
        #pprint.pprint(doc)

def extra_args(pipe):
    json_filter = {}
    if "filter" in ARGS:
        filter = ARGS["filter"]
        json_filter = json.loads(filter)    
    if json_filter != {}:
        pipe[0]["$search"]["knnBeta"]["filter"] = json_filter 
    if "dedup" in ARGS:
        pipe.append({"$group" : {
            "_id" : "$sentence", 
            "n" : {"$sum" : 1}, 
            "sentence" : {"$first" : "$sentence"},
            "url" : {"$first" : "$url"},
            "plan" : {"$first" : "$plan"},
            "score" : {"$first" : "$score"}
        }})
    if "len" in ARGS:
        pipe.append({"$match": {"$expr": {"$gte": [{"$strLenCP": "$sentence"},25]}}})
  
# --------------------------------------------------------- #
#       UTILITY METHODS
# --------------------------------------------------------- #
   
def get_vector(prompt):
    start_time = datetime.datetime.now()
    llm = "stvec768"
    vec_service = f"http://vec.dungeons.ca/{llm}/"
    query = {"text": prompt,"l2": True}
    response = requests.get(vec_service, params=query)
    vector = response.json()
    timer(start_time,1,"basic","Prompt vectorize")
    return vector

def timer(starttime,cnt = 1, ttype = "sub", prompt = "Operation took"):
    elapsed = datetime.datetime.now() - starttime
    secs = elapsed.seconds
    msecs = elapsed.microseconds
    if secs == 0:
        elapsed = msecs * .001
        unit = "ms"
    else:
        elapsed = secs + (msecs * .000001)
        unit = "s"
    if ttype == "sub":
        bb.logit(f"query ({cnt} recs) took: {'{:.3f}'.format(elapsed)} {unit}")
    elif ttype == "basic":
        bb.logit(f"{prompt}: {'{:.3f}'.format(elapsed)} {unit}")
    else:
        bb.logit(f"# --- Complete: query took: {'{:.3f}'.format(elapsed)} {unit} ---- #")
        bb.logit(f"#   {cnt} items {'{:.3f}'.format((elapsed)/cnt)} {unit} avg")

def mongodb_connection(type="uri", details={}):
    mdb_conn = settings[type]
    username = settings["username"]
    password = settings["password"]
    if "username" in details:
        username = details["username"]
        password = details["password"]
    if "secret" in password:
        password = os.environ.get("_PWD_")
    password = urllib.parse.quote_plus(password)
    mdb_conn = mdb_conn.replace("//", f"//{username}:{password}@")
    bb.logit(f"Connecting: {mdb_conn}")
    if "readPreference" in details:
        client = MongoClient(
            mdb_conn, readPreference=details["readPreference"]
        )  # &w=majority
    else:
        client = MongoClient(mdb_conn)
    return client

# --------------------------------------------------------- #
#       MAIN
# --------------------------------------------------------- #
if __name__ == "__main__":
    bb = Util()
    settings_file = "vector_demo_settings.json"
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "vector_search":
        vector_query()
    elif ARGS["action"] == "hybrid_search":
        hybrid_query()
    else:
        print(f'{ARGS["action"]} not found')

    #conn.close()
    
'''
# ---------------- DEMO ---------------------- #

Demo Document - /Users/brady.byrd/Documents/mongodb/demo/Vector_search.md

python3 vector_demo.py action=vector_search prompt="heart disease"
with filter
python3 vector_demo.py action=vector_search prompt="heart disease" filter="{\"text\":{\"path\":\"lang\",\"query\":\"eng\"}}"

[
  { '$sample': { size: 5 } },
  { '$project': { sentence: 1, plan: 1, url: 1, display_label: 1, _id: 0 } }
]


{"$search": {
            "index": "default",
            "knnBeta": {
                "vector": prompt_vector,
                "path": "sentence_vec",
                "k": num_results}
            }
        }
'''







