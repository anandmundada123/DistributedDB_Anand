#!/bin/bash

LOG="/home/hduser/cmd.log"
DBCMD="ContainerService.jar"

echo "-- [`date`] executing " >> $LOG
 
[ $# -ne 3 ] && echo "!! [`date`] Usage: $0 HOSTNAME PORT APPID" >> $LOG && exit 1

HOST=$1
PORT=$2
APPID=$3

OUTPUTFILE="output_anand"
ERRFILE="output_err"

#Move to home dir
cd /home/hduser

echo "-- [`date`] Launched: $HOST, $PORT, $APPID" >> $LOG
echo "java -jar $DBCMD $HOST $PORT $APPID 2>$ERRFILE 1>$OUTPUTFILE" >> $LOG

#Run command:
eval "java -jar $DBCMD $HOST $PORT $APPID 2>$ERRFILE 1>$OUTPUTFILE" >> $LOG

echo "-- [`date`] done:" >> $LOG


# #Make sure the output was generated
# if [ ! -s $OUTPUTFILE ]; then
#     echo "** [`date`] Output file wasn't created: $OUTPUTFILE" >> $LOG
#     exit 1
# fi

# #Now push output file into HDFS
# #First check if it is there
# if [ `hdfs dfs -ls $OUTPUTFILE 2>/dev/null | head -n 1 | grep -o "Found 1 items" | wc -l` -eq 1 ]; then
#     #Delete the file first
#     echo "** [`date`] Output file exists, removing $OUTPUTFILE" >> $LOG
#     CMD="hdfs dfs -rm -f $OUTPUTFILE"
#     $CMD 2>/dev/null
# fi

# #Now push file to HDFS
# CMD="hdfs dfs -copyFromLocal $OUTPUTFILE"
# $CMD 2>/dev/null

# #Delete the temp output file
# #rm -f $OUTPUTFILE
