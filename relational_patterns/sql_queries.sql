select cl.*, ap.firstname as ap_first, ap.lastname as ap_last, ap.gender as ap_gender, ap.dateofbirth as ap_dateofb,
op.firstname as op_first, op.lastname as op_last, op.gender as op_gender, op.dateofbirth as op_birthdate,
rp.firstname as rp_first, rp.lastname as rp_last, rp.gender as rp_gender, rp.dateofbirth as rp_birthdate,
opp.firstname as opp_first, opp.lastname as opp_last, opp.gender as opp_gender, opp.dateofbirth as opp_birthdate
from claim_claimline cl INNER JOIN provider ap on cl.attendingprovider_id = ap.provider_id
INNER JOIN provider op on cl.orderingprovider_id = op.provider_id
INNER JOIN provider rp on cl.referringprovider_id = rp.provider_id
INNER JOIN provider opp on cl.operatingprovider_id = opp.provider_id

select c.id, m.firstname, m.lastname, m.dateofbirth, m.gender, c.status, c.placeofservice, c.servicefromdate from claim c inner join member m 
ON m.member_id = c.patient_id 

select c.claim_id, m.firstname, m.lastname, m.dateofbirth, m.gender, c.claimstatus, c.placeofservice, c.servicefromdate, cl.*, 
ap.firstname as ap_first, ap.lastname as ap_last, ap.gender as ap_gender, ap.dateofbirth as ap_dateofb,
op.firstname as op_first, op.lastname as op_last, op.gender as op_gender, op.dateofbirth as op_birthdate,
rp.firstname as rp_first, rp.lastname as rp_last, rp.gender as rp_gender, rp.dateofbirth as rp_birthdate,
opp.firstname as opp_first, opp.lastname as opp_last, opp.gender as opp_gender, opp.dateofbirth as opp_birthdate
from claim_claimline cl INNER JOIN claim c on c.claim_id = cl.claim_id 
INNER JOIN member m on c.patient_id = m.member_id 
INNER JOIN provider ap on cl.attendingprovider_id = ap.provider_id
INNER JOIN provider op on cl.orderingprovider_id = op.provider_id
INNER JOIN provider rp on cl.referringprovider_id = rp.provider_id
INNER JOIN provider opp on cl.operatingprovider_id = opp.provider_id

update claim_claimline set


  [{$match: {
    Patient_id : {$in: ["M-1000004","M-1000034","M-1000024"]}
  }}, {$lookup: {
    from: 'member',
    localField: 'Patient_id',
    foreignField: 'Member_id',
    as: 'full_member'
  }}]

if "quantitativeHealth" not in doc:
    qhealth = {"deviceType" : "Aura Ring", 
        "startDate" : ISODate("2020-03-26T10:45:00Z"), 
        "AvgStepsPerDay" : "6857"}
    db.member.updateOne({_id : doc["_id"]},{$set: {"quantitativeHealth" : qhealth}})

-- Query Comparisons

select m.firstname, m.lastname, m.gender, m.dateofbirth, a.city, a.state, a.postalcode
from member m left join member_address a on m.member_id = a.member_id
where m.member_id = 'M-1000012'


db.member.find({member_id: "M-2000012"})

[
  {$match: {member_id: {$in: ["M-2000014","M-2000015","M-2000016"]}}},
  {$lookup: {
    from: "provider",
    localField: "primaryProvider_id",
    foreignField: "provider_id",
    as: "provider"
  }},
  {$unwind: "$provider"}
]

[
  {$match: {member_id: {$in: ["M-2000014","M-2000015","M-2000016"]}}},
  {$lookup: {
    from: "provider",
      {$let: {provider_id: "$primaryProvider_id"}},
      pipeline: [
        {$match: {provider_id: $$provider_id}},
        {$unwind: "$specialities"},
        {$group: {_id: "$provider_id", "specialties" : {$addToSet: "$specialties.type"}
        _id: 
      }}
      {$project: {}}
    ]
    as: "provider"
  }},
  {$unwind: "$provider"}
]
-- API
-- 
