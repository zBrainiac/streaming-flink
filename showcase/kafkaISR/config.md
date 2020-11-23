# Kafka Cluster: Insync replication (ISR), consistency across multiple regions

In case of handling very critical events you maybe have to fulfil requirements around _"zero data loss". Which is often used, but a bit misleading. It would be more precise to speak of consistend, in-sync replicated clusters.  

the following image provides an overview of ISR Kafka Cluster setup
![Alt text](../../images/KafkaISRoverview.png?raw=true "Title")

## prepare the environment
```
cd /Users/mdaeppen/infra &&
sudo yum install -y java-1.8.0-openjdk &&
sudo yum install -y wget &&
wget https://downloads.apache.org/kafka/2.6.0/kafka_2.13-2.6.0.tgz &&
tar -xzf kafka_2.13-2.6.0.tgz &&
cd kafka_2.13-2.6.0
```

## start infrastructure: 3 x Zookeeper & 2 x 5 Kafka Brokers (two data centers)
```
cd /Users/mdaeppen/infra/kafka_2.12-2.6.0
bin/zookeeper-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/zookeeper1.properties
bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server1.properties
```

## some Kafka Commands
```
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic TrafficIOTRaw 

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 5 --partitions 3 --config min.insync.replicas=4 --config unclean.leader.election.enable=true --topic TrafficIOTRaw
bin/kafka-topics.sh --create --bootstrap-server localhost:9093 --replication-factor 5 --partitions 10 --config min.insync.replicas=4 --config unclean.leader.election.enable=true --topic TrafficCounterRaw

bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic TrafficIOTRaw



bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-default --describe


bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --list

```




