#!/bin/bash
# chmod +x minifi_getlatestrelease.sh
# ./minifi_getlatestrelease.sh 0.0.0.1 minifi-rel0001.txt
# https://github.com/zBrainiac/streaming-flink/releases/download/0.0.0.1/minifi-rel0001.txt
# echo "release to download: version: $1 //filename: $2"
# rm $2
# echo "-- DONE  clean-up"
# echo "-- Download and install latest MiNiFi Release"
# export CentralKafkaBroker=localhost
wget --timeout=1 --tries=3 https://github.com/zBrainiac/streaming-flink/releases/download/"$1"/"$2" -P /tmp/

if [ "$?" -ne 0 ]; then
      msg="wget failed"
      state="nak"
      echo "$msg"
    else
      msg="wget success"
      state="ack"
fi

hostname=$(hostname -fs)
ip=1.2.3.4
CentralKafkaBroker=$CentralKafkaBroker

AckMessage='{"msg-type":"update-feedback","state":"'"$state"'","release":"'"$1-$2"'","hostname":"'"$hostname"'","SensorId":"'"$ip"'","result":"'"$msg"'"}'
echo "$AckMessage"
echo "$AckMessage" | /Users/mdaeppen/infra/kafka/bin/kafka-console-producer.sh --broker-list "$CentralKafkaBroker":9092 --topic minifi-ack

# echo "-- DONE  ALL"
