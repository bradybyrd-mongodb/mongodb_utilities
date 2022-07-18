/*
  Integrate History Function
    fires on update/insert in claim_history collection
    takes full document, looks up source claim and updates
*/
exports = async function(changeEvent) {
    //const doc_id = changeEvent.documentKey._id;
    const fullDoc = changeEvent.fullDocument;
    var changeType = changeEvent.operationType;
    // get Handle to collections
    const gClaimColl = context.services.get("M10BasicAgain").db("claimscache").collection("claims");
    const updateDescription = changeEvent.updateDescription;
    var docType = fullDoc.claimType;
    var isFound = false;
    console.log("ClaimChange: " + fullDoc["claim_id"])
    //    get the original claim
    claim = await gClaimColl.findOne({"claim_id" : fullDoc.claim_id});
    //    compare the fields, see what changed
    check_fields = ["sequence_id","claimStatus","claimStatusText","claimStatusDate","claimType","plan_id","planSubscriber_id","priorAuthorization","serviceFacility_id","serviceEndDate"];
    //    create change history doc
    change_doc = {"sequence_id" : fullDoc["sequence_id"], "updatedAt" : fullDoc.updated_at, "changes" : {}}
    for(let fld of check_fields) {
        if( fullDoc[fld] != claim[fld]) {
            console.log("Item change: " + fld);
            var newd = {"old" : claim[fld], "new" : fullDoc[fld]};
            change_doc.changes[fld] = newd;
        }
    };
    // update claim
    if( claim.claimStatusText == "initiating") {
        gClaimColl.updateOne({"claim_id" : fullDoc.claim_id},{
            $set : {"sequence_id" : fullDoc["sequence_id"] + 1, "updatedAt" : fullDoc.updated_at, claimHistory: [change_doc]}
        });
    } else {
        gClaimColl.updateOne({"claim_id" : fullDoc.claim_id},{
            $set : {"sequence_id" : fullDoc["sequence_id"] + 1, "updatedAt" : fullDoc.updated_at},
            $addToSet: {claimHistory: change_doc}
        });
    }

};  
