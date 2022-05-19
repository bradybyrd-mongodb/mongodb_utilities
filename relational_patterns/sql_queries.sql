select cl.*, ap.firstname as ap_first, ap.lastname as ap_last, ap.gender as ap_gender, ap.dateofbirth as ap_dateofb,
op.firstname as op_first, op.lastname as op_last, op.gender as op_gender, op.dateofbirth as op_birthdate,
rp.firstname as rp_first, rp.lastname as rp_last, rp.gender as rp_gender, rp.dateofbirth as rp_birthdate,
opp.firstname as opp_first, opp.lastname as opp_last, opp.gender as opp_gender, opp.dateofbirth as opp_birthdate
from claim_claimline cl INNER JOIN provider ap on cl.attendingprovider_id = ap.provider_id
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
