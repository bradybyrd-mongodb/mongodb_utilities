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
    const tables = {"claim" : {"factor" : 1, "dependent" : "claim_claimline", "d_factor" : 3}, 
              "member" : {"factor" : 0.25, "dependent" : "member_address", "d_factor" : 2},
              "provider" : {"factor" : 0.1, "dependent" : "provider_hospitaladmittingprivileges", "d_factor" : 5}
    };
    var isProcessed = false;
    const source_table = fullDoc.source_table;
    var parent_table = source_table;
    var child_table = "";
    if( parent_table in tables){
        child_table = tables[parent_table]["dependent"];
    }
    for( var item in tables){
        if( source_table == tables[item]["dependent"] ){
            parent_table = item;
        }
    };
    // get Handle to collections
    const gSourceColl = context.services.get("Claims-demo").db("healthcare").collection("claims_raw");
    const gTargetColl = context.services.get("Claims-demo").db("healthcare").collection(parent_table);
    const updateDescription = changeEvent.updateDescription;
    var isFound = false;
    console.log("ClaimChange: " + fullDoc["claim_id"]);
    //console.log(JSON.stringify(fullDoc));
    //gLogColl.insertOne(changeEvent);
    if(parent_table != source_table){ // is a child
        var p_id = parent_table + "_id";
        var c_id = parent_table + "_" + source_table + "_id";
        var test_field = source_table + "s." + c_id;
        var id = fullDoc[c_id];
        console.log("Is a " + source_table);
        var result = await gTargetColl.findOne({[test_field] : id});
        if( result != null ) {
            // Claim Exists
            console.log("Claimline - claim exists - " + result.claim_id);
            var pos_op = source_table + "s.$";
            gClaimColl.updateOne(
                {_id: result._id, [test_field] : id},
                {$set: {[pos_op] : fullDoc}}
            );
            isProcessed = true;
        }else{
            // Claim does not exist - ignore
            console.log(source_table + " - no parent - Ignore " + fullDoc[p_id]);
        }
    }else{ // its a claim
        var p_id = parent_table + "_id";
        var id = fullDoc[p_id];
        var embed = child_table + "s";
        var result = await gTargetColl.findOne({[p_id] : id});
        var newDoc = fullDoc;
        if( result) {
            // Claim Exists
            console.log(source_table + " - exists - " + result[p_id]);
            newDoc._id = result._id;
            if ( result.hasOwnProperty(embed)){
                newDoc[embed] = result[embed]; // Get existing embeds
            }else{
                var embeds = [];
                var query = {source_table: child_table, [p_id]: id, processed_at: {$exists : false}};
                gSourceColl.find(query)
                .toArray()
                .then(items => {
                    console.log(`Successfully found ${items.length} documents.`)
                    items.forEach(function(doc){
                        console.log(JSON.stringify(doc));
                        embeds.push(catchDates(doc));
                    });
                    newDoc[embed] = embeds;
                });            
            } 
            gClaimColl.replaceOne(
            {_id: result._id},
            newDoc
            );
            isProcessed = true;
        }else{ 
            // New Claim
            console.log(parent_table + " - New - " + fullDoc[p_id]);
            var embeds = [];
            var query = {source_table: child_table, [p_id]: id, processed_at: {$exists : false}};
            gSourceColl.find(query)
                .toArray()
                .then(items => {
                    console.log(`Successfully found ${items.length} documents.`)
                    items.forEach(function(doc){
                        //console.log(JSON.stringify(doc));
                        embeds.push(catchDates(doc));
                    });
                    console.log(`${embed}: ${embeds.length}`);
                    newDoc[embed] = embeds;
                    gClaimColl.insertOne(catchDates(newDoc));       
                });
            //.catch(err => console.error(`Failed to find documents: ${err}`))
            isProcessed = true;
        }
    };
    if ( isProcessed ){
      gSourceColl.updateOne({_id: fullDoc._id}, {$set: {processed_at: new Date()}});
      gSourceColl.updateMany({source_table: child_table, [p_id]: fullDoc[p_id]}, {$set: {processed_at: new Date()}});
    }
};

function catchDates(doc){
    /*
    for( const key in doc) {
        if( key.endsWith("date") || key.endsWith("_at")){
            var newd = new Date(doc[key]/1000);
            doc[key] = newd;
        }
    }
    */
    return doc;
}