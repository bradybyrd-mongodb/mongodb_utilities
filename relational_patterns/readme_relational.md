# -------------------------------------------------------#
#  Relational Replace Demo

# -------------------------------------------------------#
#  9/30/24 Side by Side comparison
Development - Springboot with Postgres and MongoDB
GOAL: A side by side video that shows progress in developing an app
Assumption: Working with MongoDB is massively more efficient, as app development
proceeds, we should see mongodb jump way out in front.
SETTING: An established Postgres application is getting a 2.0 upgrade
The upgrade involves considerable refactoring of data as well as new development
The application






Common Relational Use Cases
  One:One
  One:Many
  Many:Many
  Multi-Parent

  Common Relational Use Cases
    One:One
    One:Many
    Many:Many
    Multi-Parent

  Using standard Relational Model

  Members
    has many claims

  Providers
    has many claims

  Conditions
    has many claims
    has many members

  Claims
    has one member
    has one condition
    has one provider

  Member
    has many claims
    has many providers through claims
    has many conditions through claims

#---------------  Catalog --------------------#

      Customers
        Has many Orders
      Inventory
      - belongs to Catalog
      - subtract when order placed
      Orders
      - belongs to Customer
      - has many Items
      -- Items
        - belongs to Orders
        - belongs to Catalog

    Collections:
      Customers:
        - name
        - address
        - etc
        - recent_orders
          - order
            - items
      Orders:
        - date
        - customer
          - thumbnail
        - items
          - itemid
          - name
          - qty
          - unit_price
      Catalog:
        - item
        - description
        - unit_price
        - inventory
        - next_date


ssh -i ../../../servers/bradybyrd_mdb_key.pem ec2-user@34.207.253.18

# ----------------------------------------------------------#
#  Queries
# 7/13/22
select m.firstname, m.lastname, m.member_id, m.gender, ma.city, ma.state
from member m
left join (select member_id, city, state from member_address where member_id IN ('M-1000007','M-1000008','M-1000009','M-1000010') and name = 'Main') ma on ma.member_id = m.member_id
where m.member_id IN ('M-1000007','M-1000008','M-1000009','M-1000010');


select c.claim_id, c.claimstatus, c.claimtype, c.servicefromdate, m.firstname, m.lastname, m.dateofbirth, m.gender, cl.*, ap.firstname as ap_first, ap.lastname as ap_last, ap.gender as ap_gender, ap.dateofbirth as ap_birthdate, 
op.firstname as op_first, op.lastname as op_last, op.gender as op_gender, op.dateofbirth as op_birthdate, 
rp.firstname as rp_first, rp.lastname as rp_last, rp.gender as rp_gender, rp.dateofbirth as rp_birthdate, 
opp.firstname as opp_first, opp.lastname as opp_last, opp.gender as opp_gender, opp.dateofbirth as opp_birthdate, ma.city as city, ma.state as us_state, 
mc.phonenumber as phone, mc.emailaddress as email, me.employeeidentificationnumber as EIN
from claim c  
INNER JOIN member m on m.member_id = c.patient_id 
LEFT OUTER JOIN claim_claimline cl on cl.claim_id = c.claim_id 
INNER JOIN provider ap on cl.attendingprovider_id = ap.provider_id
INNER JOIN provider op on cl.orderingprovider_id = op.provider_id 
INNER JOIN provider rp on cl.referringprovider_id = rp.provider_id 
INNER JOIN provider opp on cl.operatingprovider_id = opp.provider_id 
LEFT JOIN (select member_id, city, state from member_address where member_id IN ('M-1000007','M-1000008','M-1000009','M-1000010') and name = 'Main') ma on ma.member_id = m.member_id 
INNER JOIN (select * from member_communication where emailtype = 'Work' and member_id IN ('M-1000007','M-1000008','M-1000009','M-1000010') limit 1) mc on mc.member_id = m.member_id
LEFT JOIN (select member_id, usage, language from member_languages where member_id IN ('M-1000007','M-1000008','M-1000009','M-1000010') and usage = 'Native') ml on ml.member_id = m.member_id 
LEFT JOIN (select member_id, employeeidentificationnumber from member_employment where member_id IN ('M-1000007','M-1000008','M-1000009','M-1000010') limit 1) me on me.member_id = m.member_id
WHERE c.patient_id IN ('M-1000007','M-1000008','M-1000009','M-1000010')


# ----------------------------------------------------------#
#  Queries
# 8/5/22

# Show a claim:
select c.*, m.firstname, m.lastname, m.dateofbirth, m.gender, cl.*, ap.firstname as ap_first, ap.lastname as ap_last, ap.gender as ap_gender, ap.dateofbirth as ap_birthdate, 
  op.firstname as op_first, op.lastname as op_last, op.gender as op_gender, op.dateofbirth as op_birthdate, 
  rp.firstname as rp_first, rp.lastname as rp_last, rp.gender as rp_gender, rp.dateofbirth as rp_birthdate, 
  opp.firstname as opp_first, opp.lastname as opp_last, opp.gender as opp_gender, opp.dateofbirth as opp_birthdate 
  from claim c  
  INNER JOIN member m on m.member_id = c.patient_id 
  LEFT OUTER JOIN claim_claimline cl on cl.claim_id = c.claim_id 
  INNER JOIN provider ap on cl.attendingprovider_id = ap.provider_id
  INNER JOIN provider op on cl.orderingprovider_id = op.provider_id 
  INNER JOIN provider rp on cl.referringprovider_id = rp.provider_id 
  INNER JOIN provider opp on cl.operatingprovider_id = opp.provider_id 
  where c.patient_id = 'M-1000004'

db.claim.findOne({patient_id: "M-1000567"})


db.claim.find({age: {$gt: 41}})

{patient_id : {$in : ['M-1000004','M-1000005','M-1000006','M-1000095','M-1000105']}}


API:
fmlumlcq/eb9329b9-65c4-429f-9b4e-e35c8ad71914
curl --request POST \
  'https://data.mongodb-api.com/app/data-amafk/endpoint/data/v1/action/find' \
  --header 'Content-Type: application/json' \
  --header 'api-key: TpqAKQgvhZE4r6AOzpVydJ9a3tB1BLMrgDzLlBLbihKNDzSJWTAHMVbsMoIOpnM6' \
  --data-raw '{
      "dataSource": "Cluster0",
      "database": "learn-data-api",
      "collection": "hello",
      "document": {
        "text": "Hello from the Data API!",
      }
  }'

  curl --location --request POST 'https://data.mongodb-api.com/app/data-amafk/endpoint/data/v1/action/findOne' \
--header 'Content-Type: application/json' \
--header 'Access-Control-Request-Headers: *' \
--header 'api-key: 621682fc4f4fa6e363ddd392' \
--data-raw '{
    "collection":"claim",
    "database":"healthcare",
    "dataSource":"M10BasicAgain",
    "projection": {"_id": 1}
}'

# ----------------------------------------------------------#
#  Presciption Model
#  1/6/23

Patient / Presciption / Provider

Same as Member/Provider
Just need prescription

# ----------------------------------------------------------#
#  Cliam-Member phi lookup
#  1/23/23

pipe = [
  {$match : {placeOfService: "School"}},
  {'$lookup': {
        'from': 'member', 
        'localField': 'patient_id', 
        'foreignField': 'member_id',  
        'as': 'member_details'
    }
  },
  {$unwind: {path: "member_details"}} 
]

pipe = [
  {$match : {version: "1.6"}},
  {$limit: 100},
  {'$lookup': {
        'from': 'claim_phi', 
        'localField': 'member_id', 
        'foreignField': 'patient_id',
        'pipeline': [
            {'$project': {'_id': 0, claim_id: 1, 'attendingProvider_id': 1, claimStatus: 1, claimStatusDate: 1, claimType: 1, placeOfService: 1, serviceFromDate: 1}}
        ],
        'as': 'recent_claims'
    }
  } 
]
# ----------------------------- #
[
  {$match: {version: '1.5', claimType: 'Dental'}},
  {$lookup: {
    from: 'member',
    localField: 'patient_id',
    foreignField: 'member_id',
    pipeline: [
      {$limit: 1}
    ],
    as: 'member'
  }}, 
  {$unwind: {path: '$member'}}, 
  {$project: {
    claimStatus: 1,
    claim_id: 1,
    placeOfService: 1,
    serviceFromDate: 1,
    age: {
      $dateDiff: {
      startDate: '$member.phi.dateOfBirth',
      endDate: '$$NOW',
      unit: 'year'
      }
    },
    claim_age: {
      $dateDiff: {
      startDate: '$serviceFromDate',
      endDate: '$$NOW',
      unit: 'day'
      }
    }
  }}, 
  {$group: {
    _id: '$placeOfService',
    avg_age: {$avg: '$age'},
    claim_age: {$avg: '$claim_age'}
  }}
]

[
  {$match: {version: '1.5'}},
  {$lookup: {
    from: 'member',
    localField: 'patient_id',
    foreignField: 'member_id',
    pipeline: [
      {$limit: 1}
    ],
    as: 'member'
  }}, 
  {$unwind: {path: '$member'}}
]
  {$project: {
    claimStatus: 1,
    claim_id: 1,
    placeOfService: 1,
    serviceFromDate: 1,
    age: {
      $dateDiff: {
      startDate: '$member.phi.dateOfBirth',
      endDate: '$$NOW',
      unit: 'year'
      }
    },
    claim_age: {
      $dateDiff: {
      startDate: '$serviceFromDate',
      endDate: '$$NOW',
      unit: 'day'
      }
    }
  }}, 
  {$group: {
    _id: '$placeOfService',
    avg_age: {$avg: '$age'},
    claim_age: {$avg: '$claim_age'}
  }}
]