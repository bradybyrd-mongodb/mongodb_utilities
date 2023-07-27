# ------------------------------------------------------------- # 
#   Demo Notes - 5/24/23

Updaters:
- db.claims_raw.findOne({source_table: "claim_claimline"})
- db.claims_raw.updateOne({source_table: "claim_claimline", claim_claimline_id: "CL-2100243"},{$set: {quantity: "bb_change0"}})
- db.claims_raw.find({source_table: "claim"})
- db.claims_raw.updateOne({source_table: "claim", claim_id: "C-2150465"},{$set: {servicefacility_id: "bb_change0"}})
- db.claims_raw.updateMany({source_table: "claim_claimline", claim_id: "C-2150465"},{$unset: {processed_at: ""}})

Provider:
    {attendingprovider_id: {$in: ["P-2000017","P-2000044","P-2000048"]}}

    































{"_id":"646cdeee4429f84304a6b600",
"claimstatusdate":1684095318000000,
"claimtype":"Dental",
"claimstatus":"382",
"placeofservice":"Residential Substance Abuse Treatment Facility",
"__deleted":"false",
"claim_id":"C-2100111",
"principaldiagnosis":"36406",
"receiveddate":1682125040000000,
"servicefacility_id":"bb_change2",
"priorauthorization":"F",
"subscriber_id":"M-77683",
"serviceenddate":1684505701000000,
"supervisingprovider_id":"P-2005411",
"attendingprovider_id":"P-2004273",
"patient_id":"M-2022727",
"renderingprovider_id":"P-2004491",
"servicefromdate":1684247036000000,
"id":112,
"modified_at":1684449186057787,
"lastxraydate":1681896024000000}


var newd = new Date(dat/1000);


{
    operatingprovider_id: 'P-2004852',
    quantity: '4',
    adjudicationdate: 1684247036000000,
    placeofservice: 'Residential Substance Abuse Treatment Facility',
    __deleted: 'false',
    claim_claimline_id: 'CL-3102111',
    claim_id: 'C-3100111',
    otheroperatingprovider_id: 'P-2005973',
    unit: 'Miles',
    procedurecode: '9263',
    serviceenddate: 1684247036000000,
    supervisingprovider_id: 'P-2005553',
    attendingprovider_id: 'P-2004884',
    orderingprovider_id: 'P-2004488',
    renderingprovider_id: 'P-2004061',
    servicefromdate: 1684247036000000,
    id: Long("21129"),
    modified_at: 1684449186057787,
    referringprovider_id: 'P-2005726'
}