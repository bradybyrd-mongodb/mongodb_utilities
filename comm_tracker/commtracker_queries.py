# Queries to perform
import re
import datetime

batches = {
    "simple" : ["activitiesResearch-simple"],
    "simple_match" : [
        "identifier_std",
        "identifier_search",
        "range_search"
    ]
}

fillers = ["investment", "citizen", "couple", "management", "husband", "various", "reveal", "without", "continue"]

# Create Queries to be executed in the performance tests
queries = {
    "base_query": {"type" : "agg", "collection" : "comm_summary", "query" :
        [
          {"$search": {
            "compound" : {
              "should" : [
                  {"regex" : {"query" : "__TERM__", "path" : "illness_history", "allowAnalyzedField": True}},
                ]
            }
          }},
          {"$project": {"score": {"$meta": "searchScore"},"patient_id": 1, "referring_physician": 1, "age": 1, "term": "creatininewhite", "illness_history": 1}}
        ]
        },
    "identifier_std": {"type" : "agg", "collection" : "comm_summary","query" :
        [
            {"$match": {"cmnctn_identifier": {"$regex" : "^COMT1000002.*"}, "taxonomy_cmnctn_format" : "Email"}},
            {"$sort": {"cmnctn_last_updated_dt" : -1}},
            {"$count": "numrecords"}
        ]
    },
    "identifier_search": {"type" : "agg", "collection" : "comm_summary","query" :
        [
        {"$search": {
            "compound": {
                "must": [
                    {"regex" : {"query" : "COMT1000002.*", "path" : "cmnctn_identifier", "allowAnalyzedField": True}},
                    {"text" : {"path" : "taxonomy_cmnctn_format", "query" : "Email"}}
                ]
            }
        }},
        {"$project": {"score": {"$meta": "searchScore"},"cmnctn_identifier": 1, "taxonomy_cmnctn_format": 1, "cmnctn_activity_dts": 1}},
        {"$count": "numrecords"}
        ]
    },
    "range_search": {"type" : "agg", "collection" : "comm_summary","query" :
        [
        {
            "$search":{
                "index": "default",
                "compound" : { 
                    "must": [
                    { 
                        "regex": { 
                            "query": "COMT369.*", 
                            "path" : "cmnctn_identifier",
                            "allowAnalyzedField": True
                        } 
                    },
                    { 
                        "range" :  { 
                            "path" : "cmnctn_activity_dts",  
                            "gte": datetime.datetime(2021,11,25,0,0,0), 
                            "lt": datetime.datetime(2021,12,31,0,0,0) 
                        } 
                    },
                    ],
                    "filter": [ 
                        { 
                            "text": { 
                                "query":  "Email", 
                                "path": "taxonomy_cmnctn_format" 
                            } 
                        }
                    ]
                } 
            } 
        },
        {
            "$sort": {"cmnctn_activity_dts": -1}
        },
        {
            "$count": "numrecords"
        }
        ]
    }
}