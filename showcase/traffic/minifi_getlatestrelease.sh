#!/bin/sh
# chmod +x minifi_getlatestrelease.sh
# ./minifi_getlatestrelease.sh 0.0.0.1 minifi-rel0001.txt
# https://github.com/zBrainiac/streaming-flink/releases/download/0.0.0.1/minifi-rel0001.txt
echo "release to download: version: $1 //filename: $2"
rm $2
echo "-- DONE  clean-up"
echo "-- Download and install latest MiNiFi Release"
wget --timeout=1 --tries=5 https://github.com/zBrainiac/streaming-flink/releases/download/$1/$2 -P /tmp/

echo "-- DONE  ALL"
