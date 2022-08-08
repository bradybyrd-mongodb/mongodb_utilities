#-------------------------------------------------------#
#  Relational Replace Demo


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
  where c.patient_id IN ('M-1000004','M-1000005','M-1000006','M-1000095','M-1000105')


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
