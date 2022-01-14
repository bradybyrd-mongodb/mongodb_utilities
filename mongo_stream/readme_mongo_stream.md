### Mongo Stream ###

Utility for replaying oplog changes back to a source server
To provide rollback capability after a migration

**Components:**
  *Runs in python3.6 or greater*
  *Requires pymongo and dnspython*
  *mongo_stream.py - the main python script for syncing*
  *mongo_stream_settings.json - settings file*
  *mongo_stream_watcher.sh - (cron job) shell script to insure that mongo_stream is running*

**Configuration:**
  in mongo_stream_settings.json set the values for your environment.
  in mongo_stream_watcher set the name of the script, homedir and email addresses

**To run the system:**
  create a cron job to run periodically (in this case every 10 minutes)
  add this line using: *sudo vi /etc/crontab*
```
{
  */10  * * * * ec2-user /home/ec2-user/mongo_stream_watcher.sh
}
```
The cron job should start the script, you can verify by:
```
{
  [ec2-user@ip-172-31-55-233 ~]$ ps -ef | grep python
  ec2-user  2979     1  0 02:30 ?        00:00:00 python3 mongo_stream.py
  ec2-user  2993  2786  0 02:37 pts/0    00:00:00 grep --color=auto python
}
```
