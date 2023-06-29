/*
  SingleView source data handler
    fires on update/insert in sources subdocument
    takes full document, looks for new/changed field and copies embeds
*/
exports = function(changeEvent) {
  //const doc_id = changeEvent.documentKey._id;
  const fullDoc = changeEvent.fullDocument;
  var changeType = changeEvent.operationType;
  var coll = 'party';
  var database = 'master_data'
  // get Handle to collections
  const gLogColl = context.services.get("MigrateDemo2").db("master_data").collection("logs");
  const gPartyColl = context.services.get("MigrateDemo2").db("master_data").collection("party");
  const updateDescription = changeEvent.updateDescription;
  var isFound = false;
  changeEvent["bbname"] = "debug";
  gLogColl.insertOne(changeEvent);
  if( ["sources"].includes(changeType)) {
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
}
