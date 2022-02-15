#!/bin/bash
#  Shell Script to check for reverse mongo process running
#  BJB 2/15/22
#  1.  Scan processes and look for python3 audit_logs_api.py
#  2.  After 15 minutes, run again

#  Start this in cron, enter into the crontab file:
#    */15  * * * * ec2-user /home/ec2-user/mongo_stream_watcher.sh

script="audit_logs_api.py";
homedir="/home/ec2-user"
# Notification Address
mailTo="brady.byrd@mongodb.com"
sender="mongo_log_download_monitor@mongodb.com"
#  Command to start script
cmd="python3 $script";
cd $homedir
if [[ $(ps -ef | grep $script | grep -v grep) ]]; then
  subject="$script - running"
  echo $subject;
  echo $subject >> $homedir/watcher_log.txt
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
   echo $subject >> $homedir/watcher_log.txt
 else
   subject="$script - ERROR: failed to start"
   echo $subject;
   echo $subject >> $homedir/watcher_log.txt
   msg="Restarting $script script - FAILED";
   MAIL_TXT="Subject: $subject\nFrom: $sender\nTo: $mailTo\n\n$msg";
   echo -e $MAIL_TXT | sendmail -t
 fi

fi
