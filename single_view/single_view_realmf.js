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
