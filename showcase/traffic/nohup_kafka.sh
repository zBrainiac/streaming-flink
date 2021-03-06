#!/bin/sh
rm *.pid
rm nohup.out
echo "clean-up done"

cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming
nohup java -classpath streaming-flink-0.4.0.0.jar producer.KafkaTrafficCollector edge2ai-1.dim.local:9092 &
echo $! > run_KafkaTrafficCollector.pid &
nohup java -classpath streaming-flink-0.4.0.0.jar producer.KafkaTrafficIOTSensor edge2ai-1.dim.local:9092 &
echo $! > run_KafkaTrafficIOTSensor.pid &
echo "start done"
