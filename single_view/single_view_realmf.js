/*
  SingleView source data handler
    fires on update/insert in sources subdocument
    takes full document, looks for new/changed field and copies embeds
*/
/*
  SingleView source data handler
    fires on update/insert in sources subdocument
    takes full document, looks for new/changed field and copies embeds
*/
exports = function(changeEvent) {
  const fullDoc = changeEvent.fullDocument;
  var changeType = changeEvent.operationType;
  // get Handle to collections
  const gLogColl = context.services.get("M10BasicAgain").db("master_data").collection("logs");
  const gPartyColl = context.services.get("M10BasicAgain").db("master_data").collection("party");
  const updateDescription = changeEvent.updateDescription;
  var isFound = false;
  gLogColl.insertOne(changeEvent);
  changeEvent.updateDescription.updatedFields.forEach(function(value, key) {
//  for (const [key, value] of changeEvent.updateDescription.updatedFields.entries()) {
    if(key.includes("sources.")){
      parts = key.split(".");
      switch(parts[1]){
        case "provider_info":
          //result = context.functions.execute("provider_change", changeEvent);
          console.log("ProviderInfo change - nothing to do");
          break;
        case "claim_customers":
          //result = context.functions.execute("medical_change", changeEvent);
          console.log("MedCustomers change");
          gPartyColl.updateOne({"_id" : changeEvent.documentKey._id}, {
            $set: {isActive: fullDocument.sources.claim_customers.is_active},
            $addToSet: {change_history: {"last_modified_at": new Date(), "source" : "claim_customers", "change" : "trigger - Add isActive field"}}
          });
          break;
        case "rx_customers":
          //result = context.functions.execute("rx_change", changeEvent);
          gPartyColl.updateOne({"_id" : changeEvent.documentKey._id}, {
            $set: {title: fullDocument.sources.rx_customers.title, suffix: fullDocument.sources.rx_customers.suffix},
            $addToSet: {change_history: {"last_modified_at": new Date(), "source" : "rx_customers", "change" : "trigger - Add title and suffix fields"}}
          });

          console.log("RxCustomers change");
          break;
      }
    }

  });
};

//--------------------------------------------------------------------//
    /*
    "updateDescription" : {
  		"updatedFields" : {
  			"sources.rx_customers.recent_claims.0.item"
    */
    console.log("id: " + fullDoc._id);
    console.log("Change: " + updateDescription);

  }
}
    //isFound = await gTargetColl.findOne({"_id" : fullDoc._id});
    console.log("id: " + fullDoc._id);
    console.log("Change: " + updateDescription);
    switch(changeType){
      case "provider_info":
        console.log("ProviderInfo change");
        break;
      case "claim_customers":
        console.log("MedCustomers change");
        break;
      case "rx_customers":
        console.log("RxCustomers change");
        break;
    }
  }
  var newDoc = {"empty" : "for now"};
  // Build Projection:
  if( docType == "Transformable") {
    newDoc = {
      "name" : fullDoc.name,
      "objectTypeID": fullDoc.objectTypeID,
    };
  } else if( docType == "SalesItem"){
      if (isFound) {
        console.log("Found Orderable");
        orderable = isFound.orderable;
      }
    newDoc = {
      "name" : fullDoc.name,
      "objectTypeID": fullDoc.objectTypeID,
      "aType" : "salesitem",
      "orderable" : orderable
      };
    } else if( docType == "Each"){
      if (isFound) {
        console.log("Found SI: " + isFound._id);
        orderable = isFound.orderable;
      }
      newDoc = {
        "name" : fullDoc.name,
        "objectTypeID": fullDoc.objectTypeID,
        "aType" : "orderable",
        "parentID": fullDoc.parentID,
      };
  }

  if (["SalesItem", "Transformable"].includes(docType)) {
    const updatedFields = updateDescription.updatedFields; // A document containing updated fields
    if(isFound ){
      gLogColl.insertOne({"LastModified" : new Date(), "objectTypeID" : "Update item from trigger", "name" : "TriggerResult", "change" : changeEvent, "targetID" : fullDoc._id, "new_doc" : newDoc});
      gTargetColl.replaceOne({ "_id" : fullDoc._id },newDoc);
    }else{
      gLogColl.insertOne({"LastModified" : new Date(), "objectTypeID" : "New item from trigger", "name" : "TriggerResult", "change" : changeEvent, "targetID" : fullDoc._id, "new_doc" : newDoc});
      gTargetColl.insertOne(newDoc);
    }
  }else if(["Each"].includes(docType)){
    if(isFound ){
      gLogColl.insertOne({"LastModified" : new Date(), "objectTypeID" : "New subitem (Each) from trigger", "name" : "TriggerResult", "change" : changeEvent, "targetID" : isFound._id, "new_doc" : newDoc});
      gTargetColl.updateOne({ "_id" : isFound._id },{"$set" : {"orderable" : newDoc}});
    }
  }
};

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
exports = function(changeEvent) {
  const fullDoc = changeEvent.fullDocument;
  var changeType = changeEvent.operationType;
  var isProcessed = false;
  // get Handle to collections
  const gSourceColl = context.services.get("Claims-demo").db("claim_demo").collection("claims_raw");
  const gClaimColl = context.services.get("Claims-demo").db("claim_demo").collection("claim");
  const updateDescription = changeEvent.updateDescription;
  var isFound = false;
  console.log("ClaimChange: " + fullDoc["claim_id"]);
  //gLogColl.insertOne(changeEvent);
  if(fullDoc.hasOwnProperty('claim_claimline_id')){ // is a claimline
    var cl_id = fullDoc["claim_claimline_id"];
    console.log("Is a Claimline");
    var result = gClaimColl.findOne({"claimlines.claim_claimline_id" : cl_id});
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
    var result = gClaimColl.findOne({claim_id : c_id});
    var newDoc = fullDoc;
    if( result.hasOwnProperty("claim_id")) {
      // Claim Exists
      console.log("Claim - claim exists - " + result.claim_id);
      if ( result.hasOwnProperty("claimlines")){
        newDoc.claimlines = result.claimlines;
      }else{
        var claimlines = [];
        var cl_result = gSourceColl.find({claim_id: c_id, processed_at: {$exists : false}, claim_claimline_id: {$exists : true}});
        for( let doc of cl_result ){
          claimlines.push(doc);
        }
        newDoc.claimlines = claimlines;
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
      var result = gSourceColl.find({claim_id: c_id, processed_at: {$exists : false}, claim_claimline_id: {$exists : true}})
      console.log(typeof result)
      while (result.hasNext()) {
        claimlines.push(result.next());
      }
      /*for( let doc of cl_result ){
        claimlines.push(doc);
      };*/
      newDoc.claimlines = claimlines;
      gClaimColl.insertOne(newDoc);
      isProcessed = true;
    }
  };
  if ( isProcessed ){
    gSourceColl.updateOne({_id: fullDoc._id}, {$set: {processed_at: new Date()}});
  }
};



// #---------------------------------------------------------- #
exports = function(changeEvent) {
  /*
    A Database Trigger will always call a function with a changeEvent.
    Documentation on ChangeEvents: https://docs.mongodb.com/manual/reference/change-events/

    Access the _id of the changed document:
    const docId = changeEvent.documentKey._id;

    Access the latest version of the changed document
    (with Full Document enabled for Insert, Update, and Replace operations):
    const fullDocument = changeEvent.fullDocument;

    const updateDescription = changeEvent.updateDescription;

    See which fields were changed (if any):
    if (updateDescription) {
      const updatedFields = updateDescription.updatedFields; // A document containing updated fields
    }

    See which fields were removed (if any):
    if (updateDescription) {
      const removedFields = updateDescription.removedFields; // An array of removed fields
    }

    Functions run by Triggers are run as System users and have full access to Services, Functions, and MongoDB Data.

    Access a mongodb service:
    const collection = context.services.get(<SERVICE_NAME>).db("db_name").collection("coll_name");
    const doc = collection.findOne({ name: "mongodb" });

    Note: In Atlas Triggers, the service name is defaulted to the cluster name.

    Call other named functions if they are defined in your application:
    const result = context.functions.execute("function_name", arg1, arg2);

    Access the default http client and execute a GET request:
    const response = context.http.get({ url: <URL> })

    Learn more about http client here: https://www.mongodb.com/docs/atlas/app-services/functions/context/#std-label-context-http
  */
};