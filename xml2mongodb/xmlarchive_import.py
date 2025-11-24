#!/usr/bin/env python3
"""
Script to run the XML to MongoDB converter

Usage:
    python run_converter.py
    python run_converter.py --file custom_archive.xml
    
"""

import argparse
import sys
import os
import json
import urllib
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from xml_to_mongodb import XMLToMongoDBConverter
from code_replacer import CodeReplacer
from config import Config

settings_file = "settings.json"

def import_xml():
    """Main function with command line argument parsing"""
    # Check if file exists
    if not os.path.exists(args.file):
        print(f"❌ Error: File '{args.file}' not found")
        sys.exit(1)
    database = settings["database"]["main"]["database"]
    collection = settings["database"]["main"]["collection"]
    
    print(f"🔄 Processing XML file: {args.file}")
    #print(f"🔗 MongoDB connection: {args.connection[:50]}...")
    print(f"🗄️  Database: {database}")
    print(f"📁 Collection: {collection}")
    print("-" * 50)
    
    # Create converter instance
    conn, db = client_connection("main")
    coll = db[collection]
    converter = XMLToMongoDBConverter(
        # dummy value here - passing direct collection object later
        connection_string = "getFromSettings"
    )
    
    # Process the XML file
    try:
        bulk_docs = []
        result = converter.process_xml_file(coll, args.file)
        cnt = 0
        for item in result:
            print(f'{cnt} {item["archive_date"]}')
            doc = strip_namespace(item)
            doc["version"] = settings["version"]
            bulk_docs.append(doc)
            cnt += 1
        
        success = coll.insert_many(bulk_docs, ordered=False)
        
        
        if success:
            print("✅ XML file successfully processed and saved to MongoDB!")
            print(f"📊 Check your MongoDB collection '{collection}' in database '{database}'")
            conn.close()
        else:
            print("❌ Failed to process XML file")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️  Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

def data_repair():
    """Repair xml crimes - this one not used"""
    database = settings["database"]["main"]["database"]
    collection = settings["database"]["main"]["collection"]
    conn, db = client_connection("main")
    coll = db[collection]
    docs = coll.find({})
    for doc in docs:
        update = {"$set": {"claim_xml": "new_value"}}
        query = {"_id": doc["_id"]}
    
    coll.update_one(query, update)
    conn.close()

def data_repair_lookups():
    """Repair xml crimes"""
    database = settings["database"]["main"]["database"]
    collection = settings["database"]["main"]["collection"]
    conn, db = client_connection("main")
    coll = db[collection]
    #try:
    # Initialize the replacer
    docs = coll.find({})
    for doc in docs:
        replacer = CodeReplacer(doc)
        replace_doc = replacer.replace_codes("display_name")
        del(replace_doc["_id"])
        replace_doc["version"] = doc["version"] + "r"
        print(f'Replacing codes in {doc["Claim"]["public_id"]} using version: {doc["version"] + "r"}')
        coll.insert_one(replace_doc)
    conn.close()

def update_lookups(doc):
    """Update lookup values"""
    new_doc = doc
    return new_doc
#----------------------------------------------------------------------#
#   Utility Routines
#----------------------------------------------------------------------#

def strip_namespace(doc):
    """
    Recursively removes namespaces from element tags and attribute keys.
    """
    new_doc = {}
    for k,v in doc.items():
        if k.startswith('//'):
            pair = k.split('}', 1)
            new_k = pair[1]
            ns = pair[0][2:]
            if isinstance(v, dict):
                v["ns"] = ns
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        item["ns"] = ns
            new_doc[new_k] = v
        else:
            new_doc[k] = v
    return new_doc

def client_connection(dtype = "uri", details = {}):
    lsettings = settings["database"]
    if dtype != "uri":
        lsettings = settings["database"][dtype]
    mdb_conn = lsettings["uri"]
    username = lsettings["username"]
    password = lsettings["password"]
    if "secret" in password:
        if "env_var" in lsettings:
            password = os.environ.get(lsettings["env_var"])
        else:
            password = os.environ.get("_PWD_")
    if "username" in details:
        username = details["username"]
        password = details["password"]
    try:
        if "%" not in password:
            password = urllib.parse.quote_plus(password)
    except Exception as e:
        print(f'Error: {e}')
        print("Set the password environment variable, e.g. _PWD_=yourpassword")
        exit(1)
    mdb_conn = mdb_conn.replace("//", f'//{username}:{password}@')
    #print(f'Connecting: {mdb_conn}')
    if "readPreference" in details:
        client = MongoClient(mdb_conn, readPreference=details["readPreference"]) #&w=majority
    else:
        client = MongoClient(mdb_conn)
        db = client[lsettings["database"]]
    return client, db

#------------------------------------------------------------------#
#     MAIN
#------------------------------------------------------------------#
if __name__ == "__main__":
    settings = None
    with open(settings_file) as jsonfile:
        settings = json.load(jsonfile)
    
    parser = argparse.ArgumentParser(
        description="Convert XML archive files to JSON and store in MongoDB"
    )
    
    parser.add_argument(
        '--file', '-f',
        type=str,
        default=Config.get_xml_file_path(),
        help=f"Path to XML file (default: {Config.get_xml_file_path()})"
    )
    parser.add_argument(
        '--action', '-a',
        type=str,
        default="import_xml",
        help=f"Action to run"
    )   
    args = parser.parse_args()
    
    if args.action == "import":
        import_xml()
    elif args.action == "repair_data":
        data_repair()
    elif args.action == "fix_codes":
        data_repair_lookups()
    