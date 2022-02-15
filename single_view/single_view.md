#-----------------------------------------------#
#   Single View Demo
#-----------------------------------------------#

# Startup Env:
    Atlas M10BasicAgain
      Database: master_data / party

    PostgreSQL
      export PATH="/usr/local/opt/postgresql@9.6/bin:$PATH"
      pg_ctl -D /usr/local/var/postgresql@9.6 start
      create database single_view with owner bbadmin;
      psql --username bbadmin single_view
      pgadmin4 installed
    Realm App:
      single_view-dtwza

#-----------------------------------------------#
#  Data Load

API Data access key: MasterParty
  master_party_customer_data/C5fopg2DUQ5rlvx89QduY4PNBTVFVGoU5JjSmzNe5KyWDmFm23R9vnFTvccWCcTY

Create Postgres Data:
  python3 single_view.py action=execute_ddl ddl=drop
  python3 single_view.py action=execute_ddl ddl=create
  python3 single_view.py action=load_pg_data
  Change num_records in the settings file to vary the amount of data

- Postgres Tables:
single_view=# \d
                  List of relations
 Schema |          Name          |   Type   |  Owner  
--------+------------------------+----------+---------
 public | claim_address          | table    | bbadmin
 public | claim_address_id_seq   | sequence | bbadmin
 public | claim_customers        | table    | bbadmin
 public | claim_customers_id_seq | sequence | bbadmin
 public | medical_claim          | table    | bbadmin
 public | medical_claim_id_seq   | sequence | bbadmin
 public | provider_info          | table    | bbadmin
 public | provider_info_id_seq   | sequence | bbadmin
 public | rx_claim               | table    | bbadmin
 public | rx_claim_id_seq        | sequence | bbadmin
 public | rx_customers           | table    | bbadmin
 public | rx_customers_id_seq    | sequence | bbadmin

#-----------------------------------------------#
#  Demo Data Load

1. Remove all the mongo stuff
  python3 single_view.py action=reset_data
2. Load the initial provider information, show the doc structure in Compass
  python3 single_view.py action=provider_sync
2. Start the report
  python3 single_view.py action=microservice
3. Now add a different data set, show the additional source
  python3 single_view.py action=claim_sync
5. Add rx_claim information, note additional name fields
  python3 single_view.py action=rx_claim_sync
6. Add related claim information
  python3 single_view.py action=rx_claim_add
  python3 single_view.py action=medical_claim_add
7. Now promote is_active to the canonical model
  python3 single_view.py action=promote_field source=claim_customers field=is_active
8. Now promote is_active to the canonical model
  python3 single_view.py action=promote_field source=claim_customers field=title,suffix


#-----------------------------------------------#
#  Data Rationalization

Principles:
  1. Preserve source data exactly
  2. Enrich data completeness with no downtime
  3.


#----------------------------------#
#  Meeting 2/11/22
Hugo Tiexeira
Mohit
Chris
Aman Ghandi
Nilesh
Sanjeev Kuls...

6-7 releases per year
  Limited by client-code delivery to the stores
  Talk about confidence -
Aman controls all database changes that get into production



#--------------------------------------------------#
#  Debug Notes

CHange Event Document
MongoDB Enterprise M10BasicAgain-shard-0:PRIMARY> db.party.updateOne({member_id: "LX100060"},{$set: {"sources.rx_customers.recent_claims.0.item": "engineer best-of-breed infrastructures xtrabb1-28"}})
{ "acknowledged" : true, "matchedCount" : 1, "modifiedCount" : 1 }
MongoDB Enterprise M10BasicAgain-shard-0:PRIMARY> db.logs.findOne({})
{
	"_id" : {
		"_data" : "8261F41FB9000000012B022C0100296E5A100446CD6FC433E145D29DFD9506543C8E5846645F6964006461F191F8B1389C52FD37FC250004"
	},
	"operationType" : "update",
	"clusterTime" : Timestamp(1643388857, 1),
	"fullDocument" : {
		"_id" : ObjectId("61f191f8b1389c52fd37fc25"),
		"member_id" : "LX100060",
		"benefit_plan_id" : "bscbsma-352595-LX100060",
		"birth_date" : ISODate("1974-06-06T00:00:00Z"),
		"first_name" : "Benjamin",
		"last_name" : "Chang",
		"phone" : "355-952-5169x770",
		"email" : "Benjamin.Chang@randomfirm.com",
		"gender" : "F",
		"address" : [
			{
				"type" : "work",
				"street" : "79806 Anthony Causeway Suite 991",
				"line2" : "",
				"city" : "Natalieville",
				"state" : "AK",
				"zipcode" : "52958"
			},
			{
				"type" : "home",
				"street" : "81869 Juan Creek Apt. 867",
				"line2" : "",
				"city" : "Lake Kristin",
				"state" : "VA",
				"zipcode" : "08465"
			}
		],
		"medical_claims" : [
			{
				"source_id" : 327,
				"member_id" : "PW100160",
				"item" : "repurpose world-class architectures",
				"message" : "New ok sing pass teacher forget race. Movement debate past give source beat possible. Key hard drop size condition.",
				"amount" : 1935,
				"created_at" : ISODate("2018-02-27T00:00:00Z"),
				"paid_status" : "paid",
				"provider" : "cigna"
			},
			{
				"source_id" : 326,
				"member_id" : "PW100160",
				"item" : "productize front-end eyeballs",
				"message" : "Lead ready left stop. Half the tough perform grow owner.",
				"amount" : 25345,
				"created_at" : ISODate("2017-06-09T00:00:00Z"),
				"paid_status" : "late",
				"provider" : "bscbsma"
			},
			{
				"source_id" : 322,
				"member_id" : "PW100160",
				"item" : "e-enable 24/7 eyeballs",
				"message" : "Degree concern industry. Election security season same follow. Time rise across speak color. Value quite tend should all. Six his everyone hair.",
				"amount" : 18475,
				"created_at" : ISODate("2017-03-04T00:00:00Z"),
				"paid_status" : "late",
				"provider" : "anthem"
			},
			{
				"source_id" : 325,
				"member_id" : "PW100160",
				"item" : "empower front-end initiatives",
				"message" : "Network executive outside debate. Admit discover tree movement set series foot thank. Couple support score.",
				"amount" : 22905,
				"created_at" : ISODate("2016-01-28T00:00:00Z"),
				"paid_status" : "billed",
				"provider" : "bscbsma"
			},
			{
				"source_id" : 323,
				"member_id" : "PW100160",
				"item" : "strategize distributed methodologies",
				"message" : "While stage describe notice film recognize place. Though will long hotel maintain base note.",
				"amount" : 12036,
				"created_at" : ISODate("2015-12-09T00:00:00Z"),
				"paid_status" : "paid",
				"provider" : "kaiser"
			},
			{
				"source_id" : 324,
				"member_id" : "PW100160",
				"item" : "orchestrate bleeding-edge e-markets",
				"message" : "Cause treatment ball return message let idea. Employee which never. Mean cultural perform public common respond manage.",
				"amount" : 21111,
				"created_at" : ISODate("2015-08-29T00:00:00Z"),
				"paid_status" : "late",
				"provider" : "kaiser"
			}
		],
		"rx_claims" : [
			{
				"source_id" : 365,
				"member_id" : "OW100260",
				"item" : "engineer best-of-breed infrastructures",
				"message" : "Responsibility difference measure. Short they option member weight buy. Reflect pass teacher tree friend air thing. Tonight mission apply crime move live spring. On month enter know.",
				"amount" : 114.676,
				"covered_amount" : 68.8055,
				"created_at" : ISODate("2018-05-25T00:00:00Z"),
				"paid_status" : "billed",
				"provider" : "aetna"
			},
			{
				"source_id" : 364,
				"member_id" : "OW100260",
				"item" : "innovate rich relationships",
				"message" : "Talk avoid imagine can along. Affect red guess assume.",
				"amount" : 171.335,
				"covered_amount" : 34.2671,
				"created_at" : ISODate("2018-03-05T00:00:00Z"),
				"paid_status" : "late",
				"provider" : "bscbsma"
			},
			{
				"source_id" : 362,
				"member_id" : "OW100260",
				"item" : "facilitate enterprise deliverables",
				"message" : "Visit natural fill member. Data cost wish enter item huge country. Material range week likely meet head bill.",
				"amount" : 27.8035,
				"covered_amount" : 11.1214,
				"created_at" : ISODate("2017-11-30T00:00:00Z"),
				"paid_status" : "paid",
				"provider" : "aetna"
			},
			{
				"source_id" : 363,
				"member_id" : "OW100260",
				"item" : "repurpose collaborative platforms",
				"message" : "Argue wish floor call idea pretty deep. Ok economic continue significant be up. Must they store offer law clear. Meeting ask everyone perform factor short agree. Particularly professional moment painting.",
				"amount" : 67.6954,
				"covered_amount" : 54.1563,
				"created_at" : ISODate("2017-11-20T00:00:00Z"),
				"paid_status" : "billed",
				"provider" : "aetna"
			},
			{
				"source_id" : 361,
				"member_id" : "OW100260",
				"item" : "scale turn-key infrastructures",
				"message" : "Ten customer reach. This pull amount remember western wrong value.",
				"amount" : 218.661,
				"covered_amount" : 43.7322,
				"created_at" : ISODate("2016-02-28T00:00:00Z"),
				"paid_status" : "late",
				"provider" : "anthem"
			},
			{
				"source_id" : 366,
				"member_id" : "OW100260",
				"item" : "enable extensible mindshare",
				"message" : "Write between series that still. What off western keep care arrive leave. Science yeah receive can choose must represent. Vote support member example quality film. Cold range lose mention degree mission better.",
				"amount" : 219.933,
				"covered_amount" : 87.9734,
				"created_at" : ISODate("2015-06-20T00:00:00Z"),
				"paid_status" : "billed",
				"provider" : "anthem"
			}
		],
		"sources" : {
			"provider_info" : {
				"source" : "provider_info",
				"source_id" : 61,
				"import_date" : ISODate("2022-01-26T13:24:56.372Z"),
				"member_id" : "LX100060",
				"benefit_plan_id" : "bscbsma-352595-LX100060",
				"birth_date" : ISODate("1974-06-06T00:00:00Z"),
				"first_name" : "Benjamin",
				"last_name" : "Chang",
				"phone" : "355-952-5169x770",
				"email" : "Benjamin.Chang@randomfirm.com",
				"gender" : "F",
				"address1_type" : "work",
				"address1_street" : "79806 Anthony Causeway Suite 991",
				"address1_line2" : "",
				"address1_city" : "Natalieville",
				"address1_state" : "AK",
				"address1_zipcode" : "52958",
				"address2_type" : "home",
				"address2_street" : "81869 Juan Creek Apt. 867",
				"address2_line2" : "",
				"address2_city" : "Lake Kristin",
				"address2_state" : "VA",
				"address2_zipcode" : "08465"
			},
			"claim_customers" : {
				"source" : "claim_customers",
				"source_id" : 61,
				"import_date" : ISODate("2022-01-26T13:25:10.337Z"),
				"member_id" : "PW100160",
				"first_name" : "Benjamin",
				"last_name" : "Chang",
				"birth_date" : ISODate("1974-06-06T10:45:00Z"),
				"email" : "Paul.Anderson@randomfirm.com",
				"gender" : "M",
				"phone" : "(704)004-0029",
				"mobile_phone" : "452.562.2580",
				"is_active" : "N",
				"created_at" : ISODate("2018-06-25T00:00:00Z"),
				"modified_at" : ISODate("2019-01-16T00:00:00Z"),
				"recent_claims" : [
					{
						"source_id" : 327,
						"member_id" : "PW100160",
						"item" : "repurpose world-class architectures",
						"message" : "New ok sing pass teacher forget race. Movement debate past give source beat possible. Key hard drop size condition.",
						"amount" : 1935,
						"created_at" : ISODate("2018-02-27T00:00:00Z"),
						"paid_status" : "paid",
						"provider" : "cigna"
					},
					{
						"source_id" : 326,
						"member_id" : "PW100160",
						"item" : "productize front-end eyeballs",
						"message" : "Lead ready left stop. Half the tough perform grow owner.",
						"amount" : 25345,
						"created_at" : ISODate("2017-06-09T00:00:00Z"),
						"paid_status" : "late",
						"provider" : "bscbsma"
					},
					{
						"source_id" : 322,
						"member_id" : "PW100160",
						"item" : "e-enable 24/7 eyeballs",
						"message" : "Degree concern industry. Election security season same follow. Time rise across speak color. Value quite tend should all. Six his everyone hair.",
						"amount" : 18475,
						"created_at" : ISODate("2017-03-04T00:00:00Z"),
						"paid_status" : "late",
						"provider" : "anthem"
					},
					{
						"source_id" : 325,
						"member_id" : "PW100160",
						"item" : "empower front-end initiatives",
						"message" : "Network executive outside debate. Admit discover tree movement set series foot thank. Couple support score.",
						"amount" : 22905,
						"created_at" : ISODate("2016-01-28T00:00:00Z"),
						"paid_status" : "billed",
						"provider" : "bscbsma"
					},
					{
						"source_id" : 323,
						"member_id" : "PW100160",
						"item" : "strategize distributed methodologies",
						"message" : "While stage describe notice film recognize place. Though will long hotel maintain base note.",
						"amount" : 12036,
						"created_at" : ISODate("2015-12-09T00:00:00Z"),
						"paid_status" : "paid",
						"provider" : "kaiser"
					},
					{
						"source_id" : 324,
						"member_id" : "PW100160",
						"item" : "orchestrate bleeding-edge e-markets",
						"message" : "Cause treatment ball return message let idea. Employee which never. Mean cultural perform public common respond manage.",
						"amount" : 21111,
						"created_at" : ISODate("2015-08-29T00:00:00Z"),
						"paid_status" : "late",
						"provider" : "kaiser"
					}
				],
				"address" : [ ]
			},
			"rx_customers" : {
				"source" : "rx_customers",
				"source_id" : 61,
				"import_date" : ISODate("2022-01-26T13:25:29.246Z"),
				"rxmem_id" : "OW100260",
				"title" : "Mr",
				"first_name" : "Benjamin",
				"last_name" : "Chang",
				"suffix" : "",
				"birth_date" : ISODate("1974-06-06T10:45:00Z"),
				"phone" : "001-138-993-0886",
				"email" : "Madeline.Richardson@randomfirm.com",
				"gender" : "F",
				"address_street" : "78201 Erica Falls Suite 405",
				"address_line2" : "",
				"address_city" : "Lake Zacharyborough",
				"address_stateprov" : "ND",
				"address_postalcode" : "93944",
				"created_at" : ISODate("2016-04-30T00:00:00Z"),
				"modified_by_id" : 194387,
				"modified_at" : ISODate("2019-04-23T00:00:00Z"),
				"recent_claims" : [
					{
						"source_id" : 365,
						"member_id" : "OW100260",
						"item" : "engineer best-of-breed infrastructures xtrabb1-28",
						"message" : "Responsibility difference measure. Short they option member weight buy. Reflect pass teacher tree friend air thing. Tonight mission apply crime move live spring. On month enter know.",
						"amount" : 114.676,
						"covered_amount" : 68.8055,
						"created_at" : ISODate("2018-05-25T00:00:00Z"),
						"paid_status" : "billed",
						"provider" : "aetna"
					},
					{
						"source_id" : 364,
						"member_id" : "OW100260",
						"item" : "innovate rich relationships",
						"message" : "Talk avoid imagine can along. Affect red guess assume.",
						"amount" : 171.335,
						"covered_amount" : 34.2671,
						"created_at" : ISODate("2018-03-05T00:00:00Z"),
						"paid_status" : "late",
						"provider" : "bscbsma"
					},
					{
						"source_id" : 362,
						"member_id" : "OW100260",
						"item" : "facilitate enterprise deliverables",
						"message" : "Visit natural fill member. Data cost wish enter item huge country. Material range week likely meet head bill.",
						"amount" : 27.8035,
						"covered_amount" : 11.1214,
						"created_at" : ISODate("2017-11-30T00:00:00Z"),
						"paid_status" : "paid",
						"provider" : "aetna"
					},
					{
						"source_id" : 363,
						"member_id" : "OW100260",
						"item" : "repurpose collaborative platforms",
						"message" : "Argue wish floor call idea pretty deep. Ok economic continue significant be up. Must they store offer law clear. Meeting ask everyone perform factor short agree. Particularly professional moment painting.",
						"amount" : 67.6954,
						"covered_amount" : 54.1563,
						"created_at" : ISODate("2017-11-20T00:00:00Z"),
						"paid_status" : "billed",
						"provider" : "aetna"
					},
					{
						"source_id" : 361,
						"member_id" : "OW100260",
						"item" : "scale turn-key infrastructures",
						"message" : "Ten customer reach. This pull amount remember western wrong value.",
						"amount" : 218.661,
						"covered_amount" : 43.7322,
						"created_at" : ISODate("2016-02-28T00:00:00Z"),
						"paid_status" : "late",
						"provider" : "anthem"
					},
					{
						"source_id" : 366,
						"member_id" : "OW100260",
						"item" : "enable extensible mindshare",
						"message" : "Write between series that still. What off western keep care arrive leave. Science yeah receive can choose must represent. Vote support member example quality film. Cold range lose mention degree mission better.",
						"amount" : 219.933,
						"covered_amount" : 87.9734,
						"created_at" : ISODate("2015-06-20T00:00:00Z"),
						"paid_status" : "billed",
						"provider" : "anthem"
					}
				]
			}
		},
		"last_update_source" : "provider_info",
		"last_modified_at" : ISODate("2022-01-26T13:24:56.372Z"),
		"version" : "1.0"
	},
	"ns" : {
		"db" : "master_data",
		"coll" : "party"
	},
	"documentKey" : {
		"_id" : ObjectId("61f191f8b1389c52fd37fc25")
	},
	"updateDescription" : {
		"updatedFields" : {
			"sources.rx_customers.recent_claims.0.item" : "engineer best-of-breed infrastructures xtrabb1-28"
		},
		"removedFields" : [ ]
	},
	"bbname" : "debug"
}
