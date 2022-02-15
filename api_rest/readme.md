##  audit_logs_api ##

#### Utility for periodically downloading logs from MongoDB atlas cluster ####

The set of python scripts pulls logs from the Atlas rest API

#### How it works: ####

The audit_logs_api.py script reads settings from the audit_logs_api.json file. Update these settings for your environment.
```
{
    "uri" : <your Atlas uri>
    "cluster_name" : <name of the cluster (not fully qualified)>,
    "version" : "1.0",
    "project_id" : <get this from your Atlas URLs like:"5d4d7ed3f2a30b18f4f88946">,
    "org_id" : <get this from your Atlas URLs like:"5e384d3179358e03d842ead1">,
    "api_private_key" : <create an API key for your Atlas project - private>,
    "api_public_key" : <create an API key for your Atlas project - public>,
    "base_url" : "https://cloud.mongodb.com/api/atlas/v1.0",
    "log_path" : <full path to where the logfiles should be stored>,
    "frequency" : 30 < the number of minutes between downloads>,
    "frequency_note" : "Note - this should match the frequency of your cron job"
}
```
The script will request the logs for each member of your replica set for the time period specified.  It will save a last_query.txt file in the logs directory.  From here on out it will pull the logs based on the last_query.txt file.
Logs will be saved in gzip format.  Hack this script if you want to push them to something like a MongoDb database of Splunk etc.
Finally, make sure the audit_logs_api.py script is executable (chmod 755 audit_logs_api.py) and call if from a cron job (/etc/crontab).
```
#  Download mongo logs every hour:
#    */0  * * * * ec2-user /home/ec2-user/audit_logs_api.py
```
