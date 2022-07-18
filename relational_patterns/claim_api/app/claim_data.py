"""This module will serve the api request."""

from config import db_client
from app import app
from bson.json_util import dumps
from flask import request, jsonify
import json
import ast
import imp
import datetime
import pprint

# Import the helpers module
helper_module = imp.load_source('*', './app/helpers.py')

# Model-specific
collection_name = "claim"
doc_type = "claim_main"

# Select the database
db = db_client.database
# Select the collection
collection = db[collection_name]

@app.route("/api/v1/claim", methods=['POST'])
def create_claim():
    """
       method to create new claim.
       """
    try:
        # Create new users
        try:
            body = ast.literal_eval(json.dumps(request.get_json()))
        except:
            # Bad request as request body is not available
            # Add message for debugging purpose
            return "", 400

        record_created = collection.insert(body)

        # Prepare the response
        if isinstance(record_created, list):
            # Return list of Id of the newly created item
            return jsonify([str(v) for v in record_created]), 201
        else:
            # Return Id of the newly created item
            return jsonify(str(record_created)), 201
    except:
        # Error while trying to create the resource
        # Add message for debugging purpose
        return "", 500


@app.route("/api/v1/claims", methods=['GET'])
def fetch_claims():
    """
       Method to fetch the claims.
    """
#    try:
    if(True):
        # Call the function to get the query params
        start = datetime.datetime.now()
        print(f"Starting {start}")
        api_result = jsonify([])
        num_results = 0
        query_params = helper_module.parse_query_params(request.query_string)
        # Check if dictionary is not empty
        if query_params:
            query = {}
            # Try to convert the value to int
            for k, v in query_params.items():
                if isinstance(v, str) and v.isdigit():
                    v = int(v)
                else:
                    v = v.decode()
                k = k.decode()
                query[k] = v

            #query = {k: int(v) if isinstance(v, str) and v.isdigit() else v for k, v in query_params.items()}

            # Fetch all the record(s)
            num_results = collection.count_documents(query)
 
            # Check if the records are found
            if num_results > 0:
                # Prepare the response
                msg = ""
                if num_results > 200:
                    msg = f'{num_results} records found, limit is 200 - try a better filter'
                result = collection.find(query).limit(200)
                print(f"Results: {num_results}")
                #print(dumps(result))
                api_result = dumps({"record count": num_results, "message": msg, "results" : result})
            else:
                # No records are found
                api_result = f'No records found for query: {dumps(query)}', 404

        # If dictionary is empty
        else:
            # Return all the records as query string parameters are not available
            num_results = 200 #collection.count_documents({})
            if num_results > 0:
                # Prepare response if the claims are found
                msg = ""
                if num_results > 200:
                    msg = f'{num_results} records found, limit is 200 - try a filter'
                result = collection.find({}).limit(200)
                print(f"Results: {num_results}")
                #print(dumps(result))
                api_result = dumps({"record count": num_results, "message": msg, "results" : result})

        end = datetime.datetime.now()
        elapsed = end - start
        secs = (elapsed.seconds) + elapsed.microseconds * .000001
        print(f"Elapsed: {format(secs,'f')} cnt: {num_results}")
        return api_result


'''
    except:
        # Error while trying to fetch the resource
        # Add message for debugging purpose
        return "", 500
'''

@app.route("/api/v1/claim/<claim_id>", methods=['GET'])
def fetch_claim_by_id(claim_id):
    """
       Function to fetch the users.
       """
#    try:
    if(True):
        # Call the function to get the query params
        print("started")
        query_params = helper_module.parse_query_params(request.query_string)
        print("got params")

        query = {"claim_id" : claim_id}

        # Fetch all the record(s)
        records_fetched = collection.find_one(query)
        print("Results: " + dumps(records_fetched))
        print("Finding: " + claim_id)
        
        # Check if the records are found
        if records_fetched is None:
            # No records are found
            return f"Nothing found for {claim_id}", 404
        else:
            # Prepare the response
            return dumps(records_fetched)


@app.route("/api/v1/claim/<claim_id>", methods=['PUT'])
def update_claim(claim_id):
    """
       Function to update the claim.
       """
    try:
        # Get the value which needs to be updated
        try:
            body = ast.literal_eval(json.dumps(request.get_json()))
        except:
            # Bad request as the request body is not available
            # Add message for debugging purpose
            return "", 400

        # Updating the user
        records_updated = collection.update_one({"id": int(claim_id)}, body)

        # Check if resource is updated
        if records_updated.modified_count > 0:
            # Prepare the response as resource is updated successfully
            return "", 200
        else:
            # Bad request as the resource is not available to update
            # Add message for debugging purpose
            return "", 404
    except:
        # Error while trying to update the resource
        # Add message for debugging purpose
        return "", 500


@app.route("/api/v1/claim/<claim_id>", methods=['DELETE'])
def remove_claim(claim_id):
    """
       Function to remove the user.
       """
    try:
        # Delete the user
        result = collection.delete_one({"id": claim_id})

        if result.deleted_count > 0 :
            # Prepare the response
            return "", 204
        else:
            # Resource Not found
            return "", 404
    except:
        # Error while trying to delete the resource
        # Add message for debugging purpose
        return "", 500


@app.errorhandler(404)
def page_not_found(e):
    """Send message to the user with notFound 404 status."""
    # Message to the user
    message = {
        "err":
            {
                "msg": "This route is currently not supported. Please refer API documentation."
            }
    }
    # Making the message looks good
    resp = jsonify(message)
    # Sending OK response
    resp.status_code = 404
    # Returning the object
    return resp
    