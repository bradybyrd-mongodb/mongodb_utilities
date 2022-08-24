/*
  Process History Function
    fires on update/insert in wellness_raw collection
    takes full document, breaks it into data documents
*/
exports = async function(changeEvent) {
    //const doc_id = changeEvent.documentKey._id;
    const fullDoc = changeEvent.fullDocument;
    const gActivityColl = context.services.get("M10BasicAgain").db("wellness").collection("activities");
    console.log("ActivityChange: " + fullDoc.id);
    data_arr = fullDoc.data;
    for(let metric of data_arr) {
        console.log("Data item: " + metric.log_id);
        gActivityColl.replaceOne({id: metric.id}, metric, {upsert : true});
    }

}
