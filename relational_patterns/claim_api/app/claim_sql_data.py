"""This module will serve the api request."""

from selectors import SelectSelector
from config import sql_client
from app import app
from helpers import sql_helper
from bson.json_util import dumps
from flask import request, jsonify
import json
import datetime
import ast
import imp
import pprint

# Import the helpers module
helper_module = imp.load_source('*', './app/helpers.py')

# Model-specific
collection_name = "claim"
doc_type = "claim_main"

# Select the database
db = sql_client.database

@app.route("/api/sql/claim", methods=['POST'])
def create_sqlclaim():
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

        #record_created = collection.insert(body)
        record_created = None

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


@app.route("/api/sql/claims", methods=['GET'])
def fetch_sqlclaims():
    """
       Method to fetch the claims.
    """
#    try:
    if(True):
        # Call the function to get the query params
        start = datetime.datetime.now()            
        print(f"Starting: {start}")
        api_result = jsonify([])
        num_results = 0
        query_params = helper_module.parse_query_params(request.query_string)
        claim_vw = table_info("claim_vw")
        base_query = claim_vw["base_query"]
        # Check if dictionary is not empty
        if query_params:
            query = ""
            for k, v in query_params.items():
                # Try to convert the value to int
                val = v if isinstance(v, str) and v.isdigit() else f'\'{v}\''
                query += f'c.{k} = {val} AND '
            query = query[0:-5]
            sql = base_query + query + " limit 200"
            # Fetch all the record(s)
            query_result = sql_helper.sql_query(sql, db)
            num_results = query_result["num_records"]
 
            # Check if the records are found
            if num_results > 0:
                # Prepare the response
                msg = ""
                if num_results > 200:
                    msg = f'{num_results} records found, limit is 200 - try a better filter'
                result = build_claim_result(db, query_result["data"])
                print(f"Results: {num_results}")
                #print(dumps(result))
                api_result = dumps({"record count": num_results, "message": msg, "results" : result})
            else:
                # No records are found
                api_result = f'No records found for query: {dumps(query)}', 404

        # If dictionary is empty
        else:
            # Return all the records as query string parameters are not available
            sql = base_query + " limit 200"
            # Fetch all the record(s)
            query_result = sql_helper.sql_query(sql, db)
            num_results = query_result["num_records"]
 
            # Check if the records are found
            if num_results > 0:
                # Prepare the response
                msg = ""
                if num_results > 200:
                    msg = f'{num_results} records found, limit is 200 - try a better filter'
                result = build_claim_result(db, query_result["data"])
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

@app.route("/api/sql/claim/<claim_id>", methods=['GET'])
def fetch_sqlclaim_by_id(claim_id):
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
        records_fetched = collection.find_one({"claim_id" : claim_id})
        print("Results: " + dumps(records_fetched))
        print("Finding: " + claim_id)
        
        # Check if the records are found
        if records_fetched is None:
            # No records are found
            return f"Nothing found for {claim_id}", 404
        else:
            # Prepare the response
            return dumps(records_fetched)


@app.route("/api/sql/claim/<claim_id>", methods=['PUT'])
def update_sqlclaim(claim_id):
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


@app.route("/api/sql/claim/<claim_id>", methods=['DELETE'])
def remove_sqlclaim(claim_id):
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

#----------------------------------------------------------#
#  Utility Methods

def table_info(tabletype):
    result = {}
    if tabletype == "claim_vw":
        base_query = "Select c.*, m.firstname, m.lastname, m.dateofbirth, m.gender, mc.phonenumber, "
        base_query += "p.firstname as ap_firstname, p.lastname as ap_lastname, p.dateofbirth as ap_dateofbirth, p.gender as ap_gender, p.nationalprovideridentifier as ap_nationalprovideridentifier "
        base_query += " from claim c INNER JOIN member m on c.patient_id = m.member_id "
        base_query += " INNER JOIN provider p on c.attendingprovider_id = p.provider_id "
        base_query += "INNER JOIN member_communication mc on mc.member_id = m.member_id "
        fields = ["id","claim_id","attendingprovider_id","claimstatus","claimstatusdate","claimtype","lastxraydate","patient_id","placeofservice","principaldiagnosis",
        "priorauthorization","receiveddate","renderingprovider_id","serviceenddate","servicefacility_id","servicefromdate",
        "subscriber_id","supervisingprovider_id",
         "firstname","lastname","dateofbirth","gender","communicationphonenumber",
        "ap_firstname","ap_lastname","ap_dateofbirth","ap_gender","ap_natinoalprovideridentitfier"]
        result = {"base_query" : base_query, "fields" : fields}
    return result

def build_claim_result(conn, records):
    # loop through claims and pull in related records
    claim_vw = table_info("claim_vw")
    claim_fields = claim_vw["fields"]
    num_records = 0
    msg = "all good"
    result = {"num_records" : num_records, "message" : msg}
    data = []
    ids = []
    rec_cnt = 0
    for row in records:
        ids.append(row[1])
    clinfo = get_claimlines(conn, ids)
    claimlines = clinfo["data"]
    for row in records:
        doc = {}
        claim_id = row[1]
        inc = 0
        for k in range(17):
            doc[claim_fields[k]] = row[k]
            inc += 1
        if claim_id in claimlines:
            doc["claimlines"] = claimlines[claim_id]
        sub_doc = {}
        for k in range(17,21):
            sub_doc[claim_fields[k]] = row[k]
        doc["patientMember"] = sub_doc
        sub_doc = {}
        for k in range(21,25):
            sub_doc[claim_fields[k]] = row[k]
        doc["attendingProvider"] = sub_doc
        data.append(doc)
        rec_cnt += 1
    return data
        
def get_claimlines(conn,claim_ids):
    payment_fields = sql_helper.column_names("claim_claimline_payment", conn)
    pfields = ', p.'.join(payment_fields)
    ids_list = '\',\''.join(claim_ids)
    sql = f'select c.*, p.{pfields} from claim_claimline c '
    sql += 'INNER JOIN claim_claimline_payment p on c.claim_claimline_id = p.claim_claimline_id '
    sql += f'WHERE c.claim_id IN (\'{ids_list}\') '
    sql += "order by c.claim_id"
    query_result = sql_helper.sql_query(sql, conn)
    num_results = query_result["num_records"]
    claimline_fields = sql_helper.column_names("claim_claimline", conn)
    num_cfields = len(claimline_fields)
    # Check if the records are found
    print(f'Claimlines: {num_results}')
    result = {"num_records" : num_results, "data" : []}
    data = {}
    if num_results > 0:
        last_id = "zzzzzz"
        firsttime = True
        rec_cnt = 0
        for row in query_result["data"]:
            cur_id = row[1]
            doc = {}
            if cur_id != last_id:
                if not firsttime:
                    data[last_id] = docs
                    docs = []
                else:
                    docs = []
                    firsttime = False
                last_id = cur_id
            #print(row)
            for k in range(num_cfields):
                #print(claimline_fields[k])
                doc[claimline_fields[k]] = row[k]
            sub_doc = {}
            for k in range(len(payment_fields)):
                #print(payment_fields[k])
                sub_doc[payment_fields[k]] = row[k + num_cfields]
            doc["payment"] = sub_doc
            docs.append(doc)
            rec_cnt += 1
        print(f'Final claimline cnt: {rec_cnt}')
            
    result["data"] = data
    return result
                   



