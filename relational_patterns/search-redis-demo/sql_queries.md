## Claim - only

``` sql
select *  from claim c where c.patient_id = 'M-2030000' 

```
## Claim + Claimlines + Claimpayments

``` sql
select c.*, cl.*
  from claim c  
  LEFT OUTER JOIN claim_claimline cl on cl.claim_id = c.claim_id 
  where c.patient_id = 'M-2030000' 

```
## Claim + Member + Provider (and a bunch of the sub tables)

``` sql
select c.*, m.firstname, m.lastname, m.dateofbirth, m.gender, cl.*, ap.firstname as ap_first, ap.lastname as ap_last, ap.gender as ap_gender, ap.dateofbirth as ap_birthdate, 
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
  LEFT JOIN member_address ma on ma.member_id = m.member_id 
  INNER JOIN (select * from member_communication where emailtype = 'Work' and member_id = 'M-2030000' limit 1) mc on mc.member_id = m.member_id
  LEFT JOIN member_languages ml on ml.member_id = m.member_id 
  LEFT JOIN member_employment me on me.member_id = m.member_id 
  INNER JOIN member_disability md on md.member_id = m.member_id 
  INNER JOIN member_guardian mg on mg.member_id = m.member_id 


  where c.patient_id = 'M-2030000' and ma.name = 'Main'

```

## Then show it in MongoDB