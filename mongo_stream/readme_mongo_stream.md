### Mongo Stream ###

Utility for replaying oplog changes back to a source server
To provide rollback capability after a migration

Components:
  +Runs in python3.6 or greater+
  +Requires pymongo and dnspython+
  +mongo_stream.py - the main python script for syncing+
  +mongo_stream_settings.json - settings file+
  +mongo_stream_watcher.sh - (cron job) shell script to insure that mongo_stream is running+
