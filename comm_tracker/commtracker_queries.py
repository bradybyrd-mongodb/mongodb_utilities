# Queries to perform
import re
batches = {
    "simple" : ["activitiesResearch-simple"],
    "simple_match" : [
        "identifier_std",
        "identifier_search"
    ]
}

fillers = ["investment", "citizen", "couple", "management", "husband", "various", "reveal", "without", "continue"]

# Create Queries to be executed in the performance tests
queries = {
    "base_query": {"type" : "agg","query" :
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
    "identifier_std": {"type" : "agg","query" :
        [
            {"$match": {"ext_taxonomy_identifier": {"$regex" : "^COMT1000002.*"}, "taxonomy_cmnctn_format" : "Email"}},
            {"$sort": {"cmnctn_last_updated_dt" : 1}},
            {"$count": "numrecords"}
        ]
    },
    "identifier_search": {"type" : "agg","query" :
        [{"$search": {
            "compound": {
                "must": [
                    {
                        "regex": {
                        "query": 'COMT1000002.*',
                        "path": 'ext_taxonomy_identifier',
                        "allowAnalyzedField": True
                        }
                    },
                    {
                        "text": {
                        "path": 'taxonomy_cmnctn_format',
                        "query": 'Email'
                        }
                    }
                ]
            }
            }}, 
            {"$project": {
                "score": {
                    "$meta": 'searchScore'
                },
                "ext_taxonomy_identifier": 1,
                "taxonomy_cmnctn_format": 1,
                "cmnctn_activity_dts": 1
            }}, 
            {"$count": 'numrecords'}]
    }
}