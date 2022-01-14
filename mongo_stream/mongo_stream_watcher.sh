#!/bin/bash
#  Shell Script to check for reverse mongo process running
#  BJB 1/6/22
#  1.  Scan processes and look for python3 mongo_stream.py
#  2.  If running, exit 0
#  3.  If not running try to restart - send email
#  4.  sleep 20secs, check again
#  5.  If not running, send email failed to start
#  Script name (python)
#  Start this in cron, enter into the crontab file:
#    */10  * * * * command

script="mongo_stream.py";
# Notification Address
mailTo="brady.byrd@mongodb.com" #"chandra.singh@point72.com"
sender="mongo_stream_monitor@point72.com"
#  Command to start script
cmd="python3 $script";

if [[ $(ps -ef | grep $script | grep -v grep) ]]; then
  subject="$script - running"
  echo $subject;
else
  subject="$script - restarting"
  msg="Attempting to restart $script"
  MAIL_TXT="Subject: $subject\nFrom: $sender\nTo: $mailTo\n\n$msg" ;
  echo -e $MAIL_TXT | sendmail -t
  #cmd="ls -l"
  $cmd &
  sleep 5;
  if [[ $(ps -ef | grep $script | grep -v grep) ]]; then
   subject="$script - started successfully"
   echo $subject;
 else
   subject="$script - ERROR: failed to start"
   echo $subject;
   msg="Restarting $script script - FAILED";
   MAIL_TXT="Subject: $subject\nFrom: $sender\nTo: $mailTo\n\n$msg";
   echo -e $MAIL_TXT | sendmail -t
 fi

fi
