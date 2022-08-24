#!/usr/bin/env python

########################################################################
#
# Many of you like to get fancy by creating separate object classes
# and external file dependencies, e.g. json files,
# I discourage you from doing that because there are file path
# reference issues that make things difficult when you containerize
# and deploy to gke. Try to keep everything in this 1 file.
# The only exception to this rule are faker models which need to be
# pre-built and tested and checked in.
#
########################################################################

# Allows us to make many pymongo requests in parallel to overcome the single threaded problem
import gevent
_ = gevent.monkey.patch_all()

########################################################################
# Add any additional imports here.
# But make sure to include in requirements.txt
########################################################################
import pymongo
from bson import json_util
from bson.json_util import loads
from bson import ObjectId
from locust import User, events, task, constant, tag, between
import time
from pickle import TRUE
from datetime import datetime, timedelta
import random
from faker import Faker
import pprint

########################################################################
# Global Static Variables that can be accessed without referencing self
# Change the connection string to point to the correct db
# and double check the readpreference etc.
########################################################################
client = None
coll = None
aggColl1 = None
aggColl2 = None
aggColl3 = None
aggColl4 = None
aggColl5 = None
# Log all application exceptions (and audits) to the same cluster
audit = None

fake = Faker()

# docs to insert per batch insert
batch_size = None

# AssetClass
my_AssetClass = ["BILL", "BOND", "CASH", "CMDTY", "FOP", "FSOPT", "FUT", "FXCFD", "OPT", "STK", "WAR"]

#nationalCompetentAuthority
my_tradeCapacity = ["DEAL", "MTCH", ""]

########################################################################
# Even though locust is designed for concurrency of simulated users,
# given how resource intensive fakers/bulk inserts are,
# you should only run 1 simulated user / worker else you'll kill the
# CPU of the workers.
########################################################################
class MetricsLocust(User):
    ####################################################################
    # Unlike a standard locust file where we throttle requests on a per
    # second basis, since we are trying to load data asap, there will
    # be no throttling
    ####################################################################

    def __init__(self, parent):
        super().__init__(parent)

        global client, coll, aggColl1, aggColl2, aggColl3, aggColl4, aggColl5, audit, batch_size

        # Singleton
        if (client is None):
            # Parse out env variables from the host
            vars = self.host.split("|")
            srv = vars[0]
            print("SRV:",srv)
            client = pymongo.MongoClient(srv)

            db = client[vars[1]]
            coll = db[vars[2]]
            # Going to hardcode the collection now since we are dealing with many colls now
            aggColl1 = db["agg1-txn1BGroupPreAgg"]
            aggColl2 = db["agg2-txn1BGroupPreAgg"]
            aggColl3 = db["agg3-txn1BGroupPreAgg"]
            aggColl4 = db["agg4-txn1BGroupPreAgg"]
            aggColl5 = db["agg5-txn1BGroupPreAgg"]

            # Log all application exceptions (and audits) to the same cluster
            audit = client.mlocust.audit

            # docs to insert per batch insert
            batch_size = int(vars[3])
            print("Batch size from Host:",batch_size)

    ################################################################
    # Example helper function that is not a Locust task.
    # All Locust tasks require the @task annotation
    # You have to pass the self reference for all helper functions
    ################################################################
    def get_time(self):
        return time.time()

    ################################################################
    # Audit should only be intended for logging errors
    # Otherwise, it impacts the load on your cluster since it's
    # extra work that needs to be performed on your cluster
    ################################################################
    def audit(self, type, msg):
        print("Audit: ", msg)
        audit.insert_one({"type":type, "ts":self.get_time(), "msg":str(msg)})

    ################################################################
    # We need to simulate 6 versions of every tx
    # this method will create a txid and will then create all versions
    ################################################################
    def generate_tx_versions(self):
        """
        Generate series of tx versions
        tx123|2/1/22/1/false
        tx123|2/2/22/2/false
        tx123|2/3/22/3/false
        tx123|2/4/22/4/false
        tx123|2/5/22/5/false
        tx123|2/6/22/6/true
        """
        # Starting variables for the tx versions
        # Random number of days (1-90 from today) to substract for payloadTs
        ldTradeDT = random.randint(1, 90)
        payloadTs = datetime.now() - timedelta(days=ldTradeDT + random.randint(1, 3))

        # submissionAccountId
        lAcctNumSM = random.randint(10, 500)
        lAcctNumLarge = random.randint(100000, 999999)
        lAcctList = [lAcctNumLarge, lAcctNumSM]
        lAcctNumA = random.choices(lAcctList, weights=(90, 10), k=1)
        lAcctNum = lAcctNumA[0]

        txNum = f'TRNXREF_{lAcctNum}_{random.randint(1, 9000000000)}'

        # payloadPosition sequence
        payloadPosition = 0

        # track if is latest
        isLatest = False

        # errors.errorCode
        ErrsubDoc = [
            {
                "fieldRejected": "BUYER_FIRST_NAMES",
                "errorCode": "E1018"
            },
            {
                "fieldRejected": "BUYER_DATE_OF_BIRTH",
                "errorCode": "E1020"
            }
            ]
        ErrsubDocEmpty = []

        RegRespDoc = [
            {
            "ruleId": f'CON-{random.randint(1, 200)}',
            "ruleDesc": "Transaction report desription123..."
            },
            {
            "ruleId": f'CON-{random.randint(201, 500)}',
            "ruleDesc": "Instrument is not a valid XYZ..."
            }
        ]
        RegRespDocDocEmpty = []

        laReportStatus = ["REPL", "CANC", "NEWM"]
        laTxnStatus = ["RAC", "RRJ", "AREJ", "REJ"]

        # array of txs that we are sending back
        txVersions = []

        # iterate through 6 times
        for x in range(6):
            # executingEntityIdCodeLei
            lLeiCodeLg = random.randint(1, 100)
            lLeiCodeSm = random.randint(1, 5)
            lLeiCode = lLeiCodeLg if lAcctNum < 6 else lLeiCodeSm
            # reportStatus ["REPL", "CANC", "NEWM"] "weights": [1, 1, 8]
            laRptSelection = random.choices(laReportStatus, weights=(1, 1, 8), k=1)
            lcRptStatus = laRptSelection[0]
            # status ["RAC", "RRJ", "AREJ", "REJ"] "weights": [1, 1, 1, 1]
            laTxnSelection = random.choices(laTxnStatus, weights=(7, 1, 1, 1), k=1)
            lcTxnStatus = laTxnSelection[0]
            lcErrors = ErrsubDoc if lcTxnStatus != "RAC" else ErrsubDocEmpty
            # regResp.ruleId
            lcRegResponse = RegRespDocDocEmpty if lcTxnStatus != "RRJ" else RegRespDoc

            if (x==5):
                isLatest = True

            document = {
                'accountName': f'Account{lAcctNum}',
                'accountId': lAcctNum,
                'pharmacistIdCode': fake.pystr(min_chars=3, max_chars=5),
                'idCodeType': fake.pystr(min_chars=5, max_chars=8),
                'entityIdCodeLei': f'LEY_{lAcctNum}_{lLeiCode}',
                "transactionReferenceNumber": txNum,
                "instructionSetKey": "TRXREFNR123635400BDQCJNMOGTBB61",
                "regulator": f'BaFIN{random.randint(1000, 9999)}',
                "reportStatus": lcRptStatus,
                "status": lcTxnStatus,
                "subStatus": f'CD{random.randint(1, 5)}',
                'payloadDt': int((payloadTs).strftime('%Y%m%d')),
                'payloadTs': payloadTs,
                "payloadPosition": payloadPosition,
                "isLatest": isLatest,
                "assetClass": my_AssetClass[random.randint(0, 10)],
                "tradingVenueTransactionIdCode": "TRXREFNR123",
                "mifidInvestmentFirm": fake.boolean(),
                "nationalCompetentAuthority": f'NCADE{random.randint(1, 10)}',
                "buyers": {
                            "buyerId": random.randint(1000, 1000000),
                            "buyerCode": f'BrCode{random.randint(1, 50)}',
                            "buyerCodeType": "LEI",
                            "buyerBranchCountry": "DE"
                          },
                "sellers": {
                            "sellerId": random.randint(1000, 1000000),
                            "sellerCode": f'SrCode{random.randint(1, 50)}',
                            "sellerCodeType": "LEI",
                            "sellerBranchCountry": "DE"
                          },
                "tradePrice": float(str(fake.pydecimal(min_value=10, max_value=10000000, right_digits=2))),
                "tradeQuantity": random.randint(100, 1000000),
                "tradeDateTime": datetime.now() - timedelta(days=ldTradeDT),
                "tradeDateTimeForSearch": 1643184780000,
                "tradeCapacity": my_tradeCapacity[random.randint(0, 2)],
                "tradePriceCurrency": "EUR",
                "tradeVenueId": "MFXR",
                "instrumentCode": f'InstrCode{random.randint(1, 100000)}',
                "executionWithinFirmCodeType": f'eFMCode{random.randint(1, 30)}',
                "supervisingBranchCountry": "DE",
                "securitiesFinancingTransactionIndicator": fake.boolean(),
                "sentToRegulator": fake.boolean(),
                "responseFromRegulator": fake.boolean(),
                "investmentDecisionWithinFirmCodeType": "",
                "investmentDecisionWithinFirmCode": f'iFMCode{random.randint(1, 30)}',
                "investmentDecisionWithinFirmCodeVersion": "",
                "fileNameId": "PRO_635400BDQCDHUOGTBB59_20220216T145725001_MIXXXXXXT.XML",
                "nonMiFidFlag": fake.boolean(),
                "warnings": "W3009",
                "warningDescriptions": "Reported - non-MiFID eligible",
                "traxWarningAdvice": "",
                "submissionSource": "Host",
                "errors": lcErrors,
                "userDefinedFields": [
                    {
                    "userDefined": f'UserFld{lAcctNum}',
                    "userDefinedValue": "value1"
                    },
                    {
                    "userDefined": f'DeptFld{lAcctNum}',
                    "userDefinedValue": "value2"
                    }
                ],
                "orderIdentifier": f'ORDERID_{lAcctNum}_{random.randint(1000, 10000000)}',
                "processingEntity": "Trax NL",
                "reports": {
                    "RegRep": "RefererenceRepID",
                    "RegRep2": "RefererenceRepID"
                },
                "regResp": lcRegResponse,
                "events": [
                    {
                    "event_timestamp": 1646217628062,
                    "status": "HLD",
                    "subStatus": "Previous transaction queued for NCA transmission",
                    "narrative": "Transaction Report received"
                    },
                    {
                    "event_timestamp": 1646217628064,
                    "status": "RAC",
                    "subStatus": "",
                    "narrative": "NCA Feedback"
                    }
                ]
            }

            # Increment sequence values
            payloadTs = payloadTs + timedelta(days=1)
            payloadPosition = payloadPosition+1

            # Add to tx versions array
            txVersions.append(document)
        return txVersions

    # TODO turn this on for the normal load
    wait_time = between(1, 1)

    ################################################################
    # Since the loader is designed to be single threaded with 1 user
    # There's no need to set a weight to the task.
    # Do not create additional tasks in conjunction with the loader
    # If you are testing running queries while the loader is running
    # deploy 2 clusters in mLocust with one running faker and the
    # other running query tasks
    # The reason why we don't want to do both loads and queries is
    # because of the simultaneous users and wait time between
    # requests. The bulk inserts can take longer than 1s possibly
    # which will cause the workers to fall behind.
    ################################################################
    # TODO 0 this out if doing normal load
    @task(1)
    def _bulkinsert(self):
        # Note that you don't pass in self despite the signature above
        tic = self.get_time();
        name = "bulkinsert";

        global coll, audit

        try:
            arr = []
            for _ in range(batch_size):
                arr.extend(self.generate_tx_versions())
            coll.insert_many(arr, ordered=False)

            events.request_success.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0)
        except Exception as e:
            events.request_failure.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            self.audit("exception", e)
            # Add a sleep for just faker gen so we don't hammer the system with file not found ex
            #time.sleep(5)
