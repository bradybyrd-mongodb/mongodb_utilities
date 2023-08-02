# Demo 1 - Query Postgres: 
python3 gcp_getclaimlines.py action=get_claims_sql cache=False patient_id='M-2030000' query=(claim or claimLinePayments or  claimMemberProvider)
# Demo 2 - Query MongoDB: 
python3 gcp_getclaimlines.py action=get_claims_mongodb patient_id='M-2030000' query=(claim or claimLinePayments or  claimMemberProvider)
# Demo 3 - Query Postgres with redis:
python3 gcp_getclaimlines.py action=get_claims_sql cache=True patient_id='M-2030000' query=(claim or claimLinePayments or  claimMemberProvider)
# Demo 4 - Transaction MongoDB with manual commit: 
python3 gcp_getclaimlines.py action=transaction_mongodb num_transactions=1 mcommit=True
# Demo 5 - Transactions Postgres:  
python3 gcp_getclaimlines.py action=transaction_postgres num_transactions=10
# Demo 6 - Transactions MongoDB:  
python3 gcp_getclaimlines.py action=transaction_mongodb num_transactions=10 mcommit=False
