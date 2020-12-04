#!/bin/sh
cd /Users/mdaeppen/infra/kafka_2.12-2.6.0 || exit
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

var=$(cat run_KafkaServer5.pid)
echo "${var}"
kill "${var}"
rm run_KafkaServer5.pid
rm run_KafkaServer5.log

var=$(cat run_KafkaServer6.pid)
echo "${var}"
kill "${var}"
rm run_KafkaServer6.pid
rm run_KafkaServer6.log

var=$(cat run_KafkaServer7.pid)
echo "${var}"
kill "${var}"
rm run_KafkaServer7.pid
rm run_KafkaServer7.log

var=$(cat run_KafkaServer8.pid)
echo "${var}"
kill "${var}"
rm run_KafkaServer8.pid
rm run_KafkaServer8.log

var=$(cat run_KafkaServer9.pid)
echo "${var}"
kill "${var}"
rm run_KafkaServer9.pid
rm run_KafkaServer9.log

var=$(cat run_KafkaServer10.pid)
echo "${var}"
kill "${var}"
rm run_KafkaServer10.pid
rm run_KafkaServer10.log

# rm -rf /Users/mdaeppen/infra/kafka_2.12-2.6.0/zkdata/zk1/version*
# rm -rf /Users/mdaeppen/infra/kafka_2.12-2.6.0/zkdata/zk2/version*
# rm -rf /Users/mdaeppen/infra/kafka_2.12-2.6.0/zkdata/zk3/version*
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

cd /Users/mdaeppen/infra/kafka_2.12-2.6.0 || exit
nohup bin/zookeeper-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/zookeeper1.properties > run_zookeeper1.log &
echo $! > run_zookeeper1.pid &
nohup bin/zookeeper-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/zookeeper2.properties > run_zookeeper2.log &
echo $! > run_zookeeper2.pid &
nohup bin/zookeeper-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/zookeeper3.properties > run_zookeeper3.log &
echo $! > run_zookeeper3.pid &
echo "start zookeeper done"
nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server1.properties > run_KafkaServer1.log &
echo $! > run_KafkaServer1.pid &
nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server2.properties > run_KafkaServer2.log &
echo $! > run_KafkaServer2.pid &
nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server3.properties > run_KafkaServer3.log &
echo $! > run_KafkaServer3.pid &
nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server6.properties > run_KafkaServer6.log &
echo $! > run_KafkaServer6.pid &
nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server7.properties > run_KafkaServer7.log &
echo $! > run_KafkaServer7.pid &
nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server8.properties > run_KafkaServer8.log &
echo $! > run_KafkaServer8.pid &
echo "start main kafka brokers done"

bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic TrafficIOTRaw
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 5 --partitions 3 --config min.insync.replicas=4 --config unclean.leader.election.enable=true --topic TrafficIOTRaw
echo "Kafka topic 'TrafficIOTRaw' created"

bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic TrafficCounterRaw
bin/kafka-topics.sh --create --bootstrap-server localhost:9093 --replication-factor 5 --partitions 10 --config min.insync.replicas=4 --config unclean.leader.election.enable=true --topic TrafficCounterRaw
echo "Kafka topic 'TrafficCounterRaw' created"


cd
cd /Users/mdaeppen/infra/cruise-control || exit
nohup ./kafka-cruise-control-start.sh config/cruisecontrol.properties > run_KafkaCruiseControl.log &
echo $! > run_KafkaCruiseControl.pid &
echo "start kafka cruise control done - http://localhost:9090/#/ "


# nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server4.properties > run_KafkaServer4.log &
# echo $! > run_KafkaServer4.pid &
# nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server5.properties > run_KafkaServer5.log &
# echo $! > run_KafkaServer5.pid &

# nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server9.properties > run_KafkaServer9.log &
# echo $! > run_KafkaServer9.pid &
# nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server10.properties > run_KafkaServer10.log &
# echo $! > run_KafkaServer10.pid &
echo "start done"