// This Code runs in an Atlas AppServices function
// It searches the Ugly Sweater catalog for matches
// based on text and vector criteria
    
exports = async function(arg){
    // To see plenty more examples of what you can do with functions see: 
    // https://www.mongodb.com/docs/atlas/app-services/functions/
  
    // Find the name of the MongoDB service you want to use (see "Linked Data Sources" tab)
    var serviceName = "mongodb-atlas";
  
    // Update these to reflect your db/collection
    var dbName = "products";
    var collName = "z-sweaters-vec";
    const vector_scalar = 0.9 
    const vector_normalization = 40 
    const fts_scalar = 1 - vector_scalar 
    const fts_normalization = 10
    
    // Get a collection from the context
    var collection = context.services.get(serviceName).db(dbName).collection(collName);
    
    const vectorSearchPipeline = await parsePromt(arg, vector_scalar, vector_normalization);
    const searchPipeline = await getSearchPipeline(arg, fts_scalar, fts_normalization);
    console.log("vectorSearchPipeline: " + JSON.stringify(vectorSearchPipeline));
    console.log("searchPipeline: " + JSON.stringify(searchPipeline));
    const unionWith = {
      "$unionWith": {
        "coll": collName, 
        "pipeline":searchPipeline
      }
    }
    
    const sort = {
      "$sort":{"score":-1 }
    }
    
    const pipeline = vectorSearchPipeline.concat([unionWith, sort]);
    //const pipeline = vectorSearchPipeline;
    var hybridSearchResult;
    try {
      hybridSearchResult = await collection.aggregate(pipeline);
    } catch(err) {
      console.log("Error occurred while executing findOne:", err.message);
      return { error: err.message };
    }
    return hybridSearchResult;
  };
  
  getSearchPipeline = async function(arg, fts_scalar, fts_normalization){
    return [
          {
            '$search': {
              'index': 'default', 
              'text': {
                'query': arg.query, 
                'path': 'title'
              }
            }
          }, 
          {
            "$unset":['img_vector','title_vector']
          },
          {
            '$addFields':{
              "source":"textSearch",
              'score': {
                '$meta': 'searchScore'
              }
            }
          },
          {
            '$set': {
              "score": {"$multiply": ["$score", fts_scalar / fts_normalization]},
            }
          }
        ]
  }
  
  parsePromt = async function (arg, vector_scalar, vector_normalization){
    const textllm = "stvec768";
    const llm = "url_image_vector";
    const num_results = 5;
    var vectorUrl = `http://vec.dungeons.ca/${textllm}`;
    var imgvectors;
    var vectors;
    var url = "";
    var findResult;
    var vectorStage = {"$match": {size: "L"}};
    var filters = [];
    
     // Find the name of the MongoDB service you want to use (see "Linked Data Sources" tab)
     var serviceName = "mongodb-atlas";
     // Update these to reflect your db/collection
     var dbName = "products";
     var collName = "z-sweaters-vec";   
     // Get a collection from the context
     var collection = context.services.get(serviceName).db(dbName).collection(collName);
    
    // sample prompt = "https://newenglandhack2024.s3.us-east-2.amazonaws.com/images/vw_bus.png large green"
    var prompt = arg.query
    // Split the prompt and look for any urls
    var prompts = prompt.split(" ");
    console.log(`Prompt: ${prompt}`)
    for(const p of prompts) {
        if( p.startsWith("http") ) {
            console.log("found url: " + p);
            url = p;
        }
    }
    prompt = prompt.replace(url,"").replaceAll(" ","%20").trim();
    
    if( url != "" ) {
        // Vectorize the prompt
        var vectorImgUrl = `http://imgvec.dungeons.ca/${llm}?image_url=${url}`;
        console.log("VectorServiceURL " + vectorImgUrl);
        const vectorResponse = await context.http.post({ url: vectorImgUrl});
        imgvectors = JSON.parse(vectorResponse.body.text());
        vectorStage = {"$vectorSearch": {
            index: "vector_index",
            queryVector: imgvectors,
            path: "img_vector",
            limit: num_results,
            numCandidates: 100
        }};
        //console.log("Vectors" + imgvectors);
    }else{
        if( prompt != "") {
            // Vectorize the text prompt
            vectorUrl = vectorUrl + "/?text=" + prompt + "&l2=false&stopwords=true"
            console.log("VectorServiceURL " + vectorUrl);
            const vectorResponse = await context.http.get({ url: vectorUrl});
            //console.log(JSON.stringify(vectorResponse));
            vectors = JSON.parse(vectorResponse.body.text());
            //console.log(JSON.stringify(vectors));
            vectorStage = {"$vectorSearch": {
                index: "vector_index",
                queryVector: vectors,
                path: "title_vector",
                limit: num_results,
                numCandidates: 5
            }};
            //console.log("Vectors" + vectors);
        }
    }
    
    // Keyword search behavior
    if( arg.gender != undefined && arg.gender != ""){
        if( arg.gender.toUpperCase().startsWith("F")){
             filters.push({gender: "Female"});
        }else{
            filters.push({gender: "Male"});
        }
    }
    if( arg.size != undefined){
        if( arg.size.toUpperCase().startsWith("L")){
             filters.push({size: "Large"});
        }else if(arg.size.toUpperCase().startsWith("M")){
            filters.push({size: "Medium"});
        }else if(arg.size.toUpperCase().startsWith("S")){
            filters.push({size: "Small"});
        }
    }
    if( arg.price != undefined){
        filters.push({price: {"$lte": arg.price[1]}});
        filters.push({price: {"$gt": arg.price[0]}});
    }
    
    if( filters.length > 0){
        var filter = {"$and" : filters}
        vectorStage["$vectorSearch"].filter = filter;
    } 
    //console.log(JSON.stringify(filter));
    //delete ["$vectorSearch"]["filter"];
    var pipe = [
        vectorStage,
        {
          "$unset":['img_vector','title_vector']
        },
        {
          "$addFields": {
            "score": {
              "$meta": "vectorSearchScore"
            },
            "source": "vectorSearch"
          }
        },
        {
          '$set': {
            "score": {"$multiply": ["$score", vector_scalar / vector_normalization]},
          }
        }
    ];
    
    return pipe;
  }