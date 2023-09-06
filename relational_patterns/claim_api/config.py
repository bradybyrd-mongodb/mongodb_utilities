"""This module is to configure app to connect with database."""
import psycopg2
from pymongo import MongoClient

#  All the settings to run this app here in plain text for all to see
settings = {
    "perfuri" : "mongodb+srv://migratedemo2.vmwqj.mongodb.net",
    "uri" : "mongodb+srv://m10basicagain-vmwqj.mongodb.net",
    "database" : "healthcare",
    "collection" : "members",
    "username" : "main_admin",
    "password" : "<secret>",
    "postgres" : {
      "host" : "localhost",
      "username" : "bbadmin",
      "password" : "<secret>",
      "database" : "healthcare",
      "notes" : "To startup - export PATH='/usr/local/opt/postgresql@9.6/bin:$PATH' pg_ctl -D /usr/local/var/postgresql@9.6 start"
    },
    "version" : "1.1",
    "process_count" : 4,
    "batch_size" : 50,
    "batches" : 2,
    "base_counter" : 1000000,
    "limitdata" : {
      "member" : {"path" : "model-tables/member.csv", "size" : 100, "id_prefix" : "M-", "thumbnail" : [
          {"name" : "recentClaims", "coll" : "claim", "type" : "many", "query" : "{}" , "find_query" : "{\"patient_id\" : doc[\"member_id\"]}", "fields" : ["claim_id","claimType","claimStatus","serviceEndDate","attendingProvider_id","placeOfService"]}
        ]}
    },
    "data" : {
      "provider" : {"path" : "model-tables/provider.csv", "size" : 50, "id_prefix" : "P-"},
      "member" : {"path" : "model-tables/member.csv", "size" : 100, "id_prefix" : "M-", "thumbnail" : [
          {"name" : "recentClaims", "coll" : "claim", "type" : "many", "query" : "{}" , "find_query" : "{\"patient_id\" : doc[\"member_id\"]}", "fields" : ["claim_id","claimType","claimStatus","serviceEndDate","attendingProvider_id","placeOfService"]}
        ]},
      "claim" : {"path" : "model-tables/claim.csv", "size" : 500, "id_prefix" : "C-", "thumbnail" : [
          {"name" : "attendingProvider", "coll" : "provider", "type" : "one", "query" : "{}", "find_query" : "{\"provider_id\" : doc[\"attendingProvider_id\"]}", "fields" : ["lastName","firstName","dateOfBirth","gender","nationalProviderIdentifier"]},
          {"name": "patientMember", "coll" : "member", "type" : "one", "query" : "{}", "find_query" : "{\"member_id\" : doc[\"patient_id\"]}", "fields" : ["lastName","firstName","dateOfBirth","gender","Communication.0.phoneNumber"]}
        ]}
    }
  }

class Db_client():

    def __init__(self, details = {}):
        self.settings = details
        self.client = self.client_connection()
        self.database = self.client[details["database"]]

    def client_connection(self, type = "uri", details = {}):
        mdb_conn = settings[type]
        username = settings["username"]
        password = settings["password"]
        mdb_conn = mdb_conn.replace("//", f'//{username}:{password}@')
        print(f'Connecting: {mdb_conn}')
        if "readPreference" in details:
            client = MongoClient(mdb_conn, readPreference=details["readPreference"]) #&w=majority
        else:
            client = MongoClient(mdb_conn)
        return client

class Sql_client():

    def __init__(self, details = {}):
        self.settings = details
        self.client = self.pg_connection()
        self.database = self.client

    def pg_connection(self, type = "postgres", sdb = 'none'):
        shost = settings[type]["host"]
        susername = settings[type]["username"]
        spwd = settings[type]["password"]
        if sdb == 'none':
            sdb = settings[type]["database"]
        conn = psycopg2.connect(
            host = shost,
            database = sdb,
            user = susername,
            password = spwd
        )
        return conn


DEBUG = True
db_client = Db_client(settings)
database = db_client.database
sql_client = Sql_client(settings)