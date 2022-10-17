# Queries to perform
import re
import datetime

batches = {
    "simple" : ["activitiesResearch-simple"],
    "simple_match" : [
        "identifier_std",
        "identifier_search",
        "range_search"
    ],
    "shards" : [
        "identifier_std_sh",
        "identifier_search_sh",
        "range_search_sh"
    ],
    "shardso" : [
        "identifier_std_sho",
        "identifier_search_sho",
        "range_search_sho"
    ],
    "std" : [
        "identifier_std_sh",
        "identifier_search_sh"
    ],
    "speed" : ["speed_test1","speed_test2","speed_test3"]
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
    },
    "identifier_std_sh": {"type" : "agg", "collection" : "comm_summary","query" :
        [
            {"$match": {"constituent_identifier": {"$regex" : "^COMT127577.*"}, "taxonomy_cmnctn_format" : "Email"}},
            {"$sort": {"cmnctn_last_updated_dt" : -1}},
            {"$count": "numrecords"}
        ]
    },
    "identifier_search_sh": {"type" : "agg", "collection" : "comm_summary","query" :
        [
        {"$search": {
            "index" : "limited",
            "compound": {
                "must": [
                    {"regex" : {"query" : "COMT127577.*", "path" : "constituent_identifier", "allowAnalyzedField": True}},
                    {"text" : {"path" : "taxonomy_cmnctn_format", "query" : "Email"}}
                ]
            }
        }},
        {"$project": {"score": {"$meta": "searchScore"},"constituent_identifier": 1, "taxonomy_cmnctn_format": 1, "cmnctn_activity_dts": 1}},
        {"$count": "numrecords"}
        ]
    },
    "range_search_sh": {"type" : "agg", "collection" : "comm_summary","query" :
        [
        {
            "$search":{
                "index": "limited",
                "compound" : { 
                    "must": [
                    { 
                        "regex": { 
                            "query": "COMT127577.*", 
                            "path" : "constituent_identifier",
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
    },
    "identifier_std_sho": {"type" : "agg", "collection" : "comm_summary","query" :
        [
            {"$match": {"cmnctn_identifier": {"$regex" : "^COMT127577.*"}, "taxonomy_cmnctn_format" : "Email"}},
            {"$sort": {"cmnctn_last_updated_dt" : -1}},
            {"$count": "numrecords"}
        ]
    },
    "identifier_search_sho": {"type" : "agg", "collection" : "comm_summary","query" :
        [
        {"$search": {
            "index" : "limited",
            "compound": {
                "must": [
                    {"regex" : {"query" : "COMT607577.*", "path" : "cmnctn_identifier", "allowAnalyzedField": True}},
                    {"text" : {"path" : "taxonomy_cmnctn_format", "query" : "Email"}}
                ]
            }
        }},
        {"$project": {"score": {"$meta": "searchScore"},"constituent_identifier": 1,"cmnctn_identifier": 1, "taxonomy_cmnctn_format": 1, "cmnctn_activity_dts": 1}},
        {"$count": "numrecords"}
        ]
    },
    "range_search_sho": {"type" : "agg", "collection" : "comm_summary","query" :
        [
        {
            "$search":{
                "index": "limited",
                "compound" : { 
                    "must": [
                    { 
                        "regex": { 
                            "query": "COMT969554.*", 
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
    },
    "speed_test1": {"type" : "find", "collection": "comm_summary", "project": {"_id" : 0, "cmnctn_identifier":1,"taxonomy_cmnctn_format":1, "cmnctn_activity_dts": 1 }, "limit": 100, "query" :
        {"cmnctn_identifier" : {"$regex" : "^COMT60007301.*"}}
    },
    "speed_test2": {"type" : "find", "collection": "comm_summary", "project": {"_id" : 0, "cmnctn_identifier":1,"taxonomy_cmnctn_format":1, "cmnctn_activity_dts": 1 }, "limit": 100, "query" :
        {"cmnctn_identifier" : {"$regex" : "^COMT50007301.*"}}
    },
    "speed_test3": {"type" : "find", "collection": "comm_summary", "project": {"_id" : 0, "cmnctn_identifier":1,"taxonomy_cmnctn_format":1, "cmnctn_activity_dts": 1 }, "limit": 100, "query" :
{"cmnctn_identifier" : {"$regex" : "^COMT30007301.*"}}
    },
    "speed_test4": {"type" : "find", "collection": "comm_summary", "project": {"_id" : 0, "cmnctn_identifier":1,"taxonomy_cmnctn_format":1, "cmnctn_activity_dts": 1 }, "limit": 100, "query" :
{"cmnctn_identifier" : "COMT50007301_41~2650905334^MEA^39483_1000000000012_jjjjjjjjj_COM^67.3^ngtdc commercial non a1a intervention^CT-0000000588^HEE"}
    },
    "speed_test5": {"type" : "find", "collection": "comm_summary", "project": {"_id" : 0, "cmnctn_identifier":1,"taxonomy_cmnctn_format":1, "cmnctn_activity_dts": 1 }, "limit": 100, "query" :
{"cmnctn_identifier" : "COMT60007000_41~2650905334^MEA^39483_1000000000012_jjjjjjjjj_COM^67.3^ngtdc commercial non a1a intervention^CT-0000000588^HEE"}
    }
    
}