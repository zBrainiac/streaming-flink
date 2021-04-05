#!/bin/sh

cd /Users/mdaeppen/infra/kafka_2.12-2.2.1 || exit
var=$(cat run_KafkaServer1.pid)
echo "${var}"
kill "${var}"
rm run_KafkaServer1.pid
rm run_KafkaServer1.log

var=$(cat run_KafkaServer2.pid)
echo "${var}"
kill "${var}"
rm run_KafkaServer2.pid
rm run_KafkaServer2.log

var=$(cat run_KafkaServer3.pid)
echo "${var}"
kill "${var}"
rm run_KafkaServer3.pid
rm run_KafkaServer3.log

var=$(cat run_KafkaServer4.pid)
echo "${var}"
kill "${var}"
rm run_KafkaServer4.pid
rm run_KafkaServer4.log

var=$(cat run_zookeeper1.pid)
echo "${var}"
kill "${var}"
rm run_zookeeper1.pid
rm run_zookeeper1.log

var=$(cat run_zookeeper2.pid)
echo "${var}"
kill "${var}"
rm run_zookeeper2.pid
rm run_zookeeper2.log

var=$(cat run_zookeeper3.pid)
echo "${var}"
kill "${var}"
rm run_zookeeper3.pid
rm run_zookeeper3.log

echo "clean-up done"

#cd /Users/mdaeppen/infra/kafka_2.12-2.6.0 || exit
#nohup bin/zookeeper-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaMigration/config/zookeeper1.properties > run_zookeeper1.log &
#echo $! > run_zookeeper1.pid &
#nohup bin/zookeeper-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaMigration/config/zookeeper2.properties > run_zookeeper2.log &
#echo $! > run_zookeeper2.pid &
#nohup bin/zookeeper-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaMigration/config/zookeeper3.properties > run_zookeeper3.log &
#echo $! > run_zookeeper3.pid &
#echo "start zookeeper done"
#nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaMigration/config/server1.properties > run_KafkaServer1.log &
#echo $! > run_KafkaServer1.pid &
#nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaMigration/config/server2.properties > run_KafkaServer2.log &
#echo $! > run_KafkaServer2.pid &
#nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaMigration/config/server3.properties > run_KafkaServer3.log &
#echo $! > run_KafkaServer3.pid &
#echo "start main kafka brokers done"
#
#bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic TrafficIOTRaw
#bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 5 --partitions 3 --topic iot
#echo "Kafka topic 'TrafficIOTRaw' created"
#
#echo "start done"