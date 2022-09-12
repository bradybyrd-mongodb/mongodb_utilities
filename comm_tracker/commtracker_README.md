# ---- Data Load ---------------------#
python3 claimcache.py action=customer_load
python3 claimcache.py action=recommendations_load
python3 claimcache_pbm.py action=load_claim_updates test=true size=10

{"user.user_id" : {$in: ["PROF1000083","PROF1000107","PROF1000123","PROF1000244","PROF1000255","PROF1000702"]}}

python3 commtracker.py action=publish_direct > output9-11.txt 2>&1 &

INdexes:
db.comm_summary.createIndex({
    cmnctn_identifier: 1, taxonomy_cmnctn_format: 1, cmnctn_activity_dts: 1
})
db.comm_detail.createIndex({
    cmnctn_identifier: 1, taxonomy_cmnctn_format: 1, cmnctn_activity_dts: 1
})
cmnctn_identifier, cmnctn_activity_dts, taxonomy_cmnctn_format

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
    {$match: {"ext_taxonomy_identifier": {"$regex" : "^COMT1000002.*"}, "taxonomy_cmnctn_format" : "Email"}},
    {$sort: {"cmnctn_last_updated_dt" : 1}},
    {$count: "num_recs"}
]
# --- Search
[
    {$search: {
        compound: {
            must: [
                {"regex" : {"query" : "^COMT1000002.*", "path" : "ext_taxonomy_identifier", "allowAnalyzedField": true}},
                {"text" : {"path" : "taxonomy_cmnctn_format", "query" : "Email"}}
            ]
        }
    }},
    {"$project": {"score": {"$meta": "searchScore"},"ext_taxonomy_identifier": 1, "taxonomy_cmnctn_format": 1, "cmnctn_activity_dts": 1}},
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
