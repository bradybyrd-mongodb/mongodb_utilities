/*     Claim from Kafka   
  Claim change source data handler
    fires on update/insert in claim_raw collection
    takes full document, creates embedded claimlines

    Plan:
      Receive change:
        is claimline - does claim exist?
          yes - update it - mark as processed
          no - ignore
        is claim - does exist?
          yes - update it - mark as processed
          no - create it - look for claimlines, embed
*/
exports = async function(changeEvent) {
    const fullDoc = changeEvent.fullDocument;
    var changeType = changeEvent.operationType;
    var isProcessed = false;
    // get Handle to collections
    const gSourceColl = context.services.get("Claims-demo").db("claim_demo").collection("claims_raw");
    const gClaimColl = context.services.get("Claims-demo").db("claim_demo").collection("claim");
    const updateDescription = changeEvent.updateDescription;
    var isFound = false;
    console.log("ClaimChange: " + fullDoc["claim_id"]);
    //console.log(JSON.stringify(fullDoc));
    //gLogColl.insertOne(changeEvent);
    if(fullDoc.hasOwnProperty('claim_claimline_id')){ // is a claimline
      var cl_id = fullDoc["claim_claimline_id"];
      console.log("Is a Claimline");
      var result = await gClaimColl.findOne({"claimlines.claim_claimline_id" : cl_id});
      if( result.hasOwnProperty("claim_id")) {
        // Claim Exists
        console.log("Claimline - claim exists - " + result.claim_id);
        gClaimColl.updateOne(
          {_id: result._id, "claimlines.claim_claimline_id" : cl_id},
          {$set: {"claimlines.$" : fullDoc}}
        );
        isProcessed = true;
      }else{
        // Claim does not exist - ignore
        console.log("Claimline - no claim - Ignore " + fullDoc.claim_id);
      }
    }else{ // its a claim
      var c_id = fullDoc["claim_id"];
      var result = await gClaimColl.findOne({claim_id : c_id});
      var newDoc = fullDoc;
      if( result.hasOwnProperty("claim_id")) {
        // Claim Exists
        console.log("Claim - claim exists - " + result.claim_id);
        newDoc._id = result._id;
        if ( result.hasOwnProperty("claimlines")){
            newDoc.claimlines = result.claimlines;
        }else{
            var claimlines = [];
            const query = {claim_id: c_id, processed_at: {$exists : false}, claim_claimline_id: {$exists : true}};
            gSourceColl.find(query)
            .toArray()
            .then(items => {
                console.log(`Successfully found ${items.length} documents.`)
                items.forEach(function(doc){
                    console.log(JSON.stringify(doc));
                    claimlines.push(catchDates(doc));
                });
                newDoc.claimlines = claimlines;
            });
            
        } 
        gClaimColl.replaceOne(
          {_id: result._id},
          newDoc
        );
        isProcessed = true;
      }else{ 
        // New Claim
        console.log("Claim - New - " + fullDoc.claim_id);
        var claimlines = [];
        const query = {claim_id: c_id, processed_at: {$exists : false}, claim_claimline_id: {$exists : true}};
        gSourceColl.find(query)
            .toArray()
            .then(items => {
                console.log(`Successfully found ${items.length} documents.`)
                items.forEach(function(doc){
                    //console.log(JSON.stringify(doc));
                    claimlines.push(catchDates(doc));
                });
                console.log(`claimlines: ${claimlines.length}`);
                newDoc.claimlines = claimlines;
                gClaimColl.insertOne(catchDates(newDoc));       
            });
        //.catch(err => console.error(`Failed to find documents: ${err}`))
        isProcessed = true;
      }
    };
    if ( isProcessed ){
      gSourceColl.updateOne({_id: fullDoc._id}, {$set: {processed_at: new Date()}});
      gSourceColl.updateMany({claim_id: fullDoc.claim_id, claim_claimline_id: {$exists: true}}, {$set: {processed_at: new Date()}});
    }
};

function catchDates(doc){
    for( const key in doc) {
        if( key.endsWith("date") || key.endsWith("_at")){
            var newd = new Date(doc[key]/1000);
            doc[key] = newd;
        }
    }
    return doc;
}