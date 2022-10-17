# ---- Data Load ---------------------#
python3 claimcache.py action=customer_load
python3 claimcache.py action=recommendations_load
python3 claimcache_pbm.py action=load_claim_updates test=true size=10

{"user.user_id" : {$in: ["PROF1000083","PROF1000107","PROF1000123","PROF1000244","PROF1000255","PROF1000702"]}}

python3 commtracker.py action=publish_direct > output9-11.txt 2>&1 &

mongosh "mongodb+srv://commstracker.mhsdb.mongodb.net/commtracker" --apiVersion 1 --username main_admin --password bugsyBoo

APIkey: qzllcgrg:37c9c00b-21c0-43e9-b097-36426f700142

python3 commtracker.py action=publish_direct > output-09-14-22.txt 2>&1 &
python3 commtracker.py action=publish_kafka > output-09-16-22.txt 2>&1 &

# --- Sharded Cluster --- #
mongosh "mongodb+srv://commsharded.mhsdb.mongodb.net/commtracker" --apiVersion 1 --username main_admin --password bugsyBoo
M40 3 shards, 500Gb, 3 region

ShardKey: taxonomy_cmnctn_format, id
Atlas [mongos] commtracker> sh.enableSharding("commtracker")
{
  ok: 1,
  '$clusterTime': {
    clusterTime: Timestamp({ t: 1663180948, i: 2 }),
    signature: {
      hash: Binary(Buffer.from("99d350367cda83822d5d908aaaa24a75904dc48b", "hex"), 0),
      keyId: Long("7143073664617938948")
    }
  },
  operationTime: Timestamp({ t: 1663180948, i: 2 })
}

sh.shardCollection("commtracker.comm_detail", { constituent_indentifier: "hashed" } )
sh.shardCollection("commtracker.comm_summary", { constituent_indentifier: "hashed" } )
Atlas [mongos] commtracker> sh.shardCollection("commtracker.comm_detail", { constituent_indentifier: 1 } )
{
  collectionsharded: 'commtracker.comm_detail',
  ok: 1,
  '$clusterTime': {
    clusterTime: Timestamp({ t: 1663187243, i: 19 }),
    signature: {
      hash: Binary(Buffer.from("302dad912359304b3dfbac5a9aa8a882b23f2952", "hex"), 0),
      keyId: Long("7143073664617938948")
    }
  },
  operationTime: Timestamp({ t: 1663187243, i: 15 })
}

# ----------------------------------------- #
#  Index Creation

db.createCollection("comm_summary")
db.createCollection("comm_detail")
db.comm_summary.createIndex({
    constituent_identifier: 1, taxonomy_cmnctn_format: 1, cmnctn_activity_dts: 1
})
db.comm_detail.createIndex({
    constituent_identifier: 1, taxonomy_cmnctn_format: 1, cmnctn_activity_dts: 1
})

db.comm_detail.createIndex({
  "is_medicaid": 1,
  "version": 1
})

db.comm_summary.createIndex({
  "is_medicaid": 1,
  "version": 1
})

db.comm_summary.createIndex({
  "constituent_identifier": "hashed"
})

db.comm_detail.createIndex({
  "constituent_identifier": "hashed"
})

180 days = 86400*180
db.comm_summary.createIndex({
  "archive_date": 1
},{expireAfterSeconds: 15552000})


db.comm_detail.createIndex({
  "archive_date": 1
},{expireAfterSeconds: 15552000})

db.comm_summary.reindex({})
sh.shardCollection("commtracker.comm_summary", { constituent_identifier: "hashed"}, {numInitialChunks: 3125 } )
sh.shardCollection("commtracker.comm_detail", { constituent_identifier: "hashed"}, {numInitialChunks: 3125 } )

python3 commtracker.py action=publish_direct > output-09-14-22.txt 2>&1 &

db.comm_summary.getShardDistribution()

# --------------------------------------- #
#  Speed Test
version: 2.2st
db.comm_detail.createIndex({
    constituent_identifier: 1, taxonomy_cmnctn_format: 1, cmnctn_activity_dts: 1
})



# ----------------------------------------- #
#  Data API
https://data.mongodb-api.com/app/data-gseer/endpoint/data/v1
API-Key
bb-poc-api/ aNB1gCJZU0R8T4HQKlpVXclJysY9sX2JcwAaXGwPnXCDymKc2bKzThL71JjyY8Wt

curl --location --request POST 'https://data.mongodb-api.com/app/data-gseer/endpoint/data/v1/action/findOne' \
--header 'Content-Type: application/json' \
--header 'Access-Control-Request-Headers: *' \
--header 'api-key: aNB1gCJZU0R8T4HQKlpVXclJysY9sX2JcwAaXGwPnXCDymKc2bKzThL71JjyY8Wt' \
--data-raw '{
    "collection":"<COLLECTION_NAME>",
    "database":"<DATABASE_NAME>",
    "dataSource":"CommsTracker",
    "projection": {"_id": 1}
}'

https://data.mongodb-api.com/app/data-gseer/endpoint/data/v1/action/findOne
{
    "collection":"comm_summary",
    "database":"commtracker",
    "dataSource":"CommsTracker",
    "projection": {"_id": 1}
}

https://data.mongodb-api.com/app/data-gseer/endpoint/data/v1/action/findOne
{
    "collection":"comm_summary",
    "database":"commtracker",
    "dataSource":"CommsTracker",
    "filter": {"cmnctn_identifier": {"$regex" : "^}}
}

# --------------------------------------------------- #
# Query Examples - 9/6/22

Index: ext_taxonomy_identifier,  taxonomy_cmnctn_format

Query:
[
    {$match: {"cmnctn_identifier": {"$regex" : "^COMT1000002.*"}, "taxonomy_cmnctn_format" : "Email"}},
    {$sort: {"cmnctn_last_updated_dt" : 1}},
    {$count: "num_recs"}
]
# --- Search
[
    {$search: {
        compound: {
            must: [
                {"regex" : {"query" : "COMT1000002.*", "path" : "cmnctn_identifier", "allowAnalyzedField": true}},
                {"text" : {"path" : "taxonomy_cmnctn_format", "query" : "Email"}}
            ]
        }
    }},
    {"$project": {"score": {"$meta": "searchScore"},"cmnctn_identifier": 1, "taxonomy_cmnctn_format": 1, "cmnctn_activity_dts": 1}},
    {"$count": "numrecords"}
]

[
  {
    $search: {
      index: 'default',
      text: {
        query: 'COMT1000002',
        path: {
          'wildcard': '*'
        }
      }
    }
  }
]

[
  {
    $search: {
      index: 'default',
      text: {
        query: 'COMT1000002',
        path: "ext_taxonomy_format"
      }
    }
  }
]

[{$search: {
  compound: {
    must: [
      {
        regex: {
          query: 'COMT1000002.*',
          path: 'ext_taxonomy_identifier',
          allowAnalyzedField: true
        }
      },
      {
        text: {
          path: 'taxonomy_cmnctn_format',
          query: 'Email'
        }
      }
    ]
  }
}}, {$project: {
  score: {
    $meta: 'searchScore'
  },
  ext_taxonomy_identifier: 1,
  taxonomy_cmnctn_format: 1,
  cmnctn_activity_dts: 1
}}, {$count: 'numrecords'}]


[
   {
      $search:{
         index: "default",
         compound : { 
            "must": [
               { 
                  "regex": { 
                      "query": "COMT369.*", 
                      "path" : "cmnctn_identifier",
                      "allowAnalyzedField": true
                  } 
               },
               { 
                  range :  { 
                     path : "cmnctn_activity_dts",  
                     "gte": ISODate("2021-11-25T00:00:00.000Z"), 
                     "lt": ISODate("2021-12-02T00:00:00.000Z") 
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
      $sort: {"cmnctn_activity_dts": -1}
   },
   {
      $limit: 50
   }
]

Date range:
[
   {
      $search:{
         index: "default",
         compound : { 
            "must": [
               { 
                  range :  { 
                     path : "cmnctn_activity_dts",  
                     "gte": ISODate("2021-11-25T00:00:00.000Z"), 
                     "lt": ISODate("2021-12-02T00:00:00.000Z") 
                  } 
               },
            ]
        } 
    } 
   },
   {
      $count: "numrecords"
   }
]

#   Set the Archive Date
db.comm_detail.updateMany({},{$currentDate : {"archive_date" : true}})
db.comm_summary.updateMany({},{$currentDate : {"archive_date" : true}})
# -------------------------------------------------------- #
#  Materialized View

- Summary
Started build 6:38
[
    {$match: { is_medicaid: "N"}},
    {$merge: {
         into: "vw_summary_non_medicaid",
         on: "_id",
         whenMatched: "replace",
         whenNotMatched: "insert"
      }
   }
]

- Detail
Started build - 6:27pm - done 6:34 1.7M rows
[
    {$match: { is_medicaid: "N"}},
    {$merge: {
         into: "vw_detail_non_medicaid",
         on: "_id",
         whenMatched: "replace",
         whenNotMatched: "insert"
      }
   }
]

- create view
db.createView(
    "vw_nonmedicaid",
    "comm_summary",
    [{$match: {is_medicaid: "N"}}]
)


- Update 11:40 - 60million new
db.comm_summary.aggregate(pipe)

# ----------------------------------------- #
#  Rest API

ProcessID: 820acde8445dc943b6d28e986798ee02
