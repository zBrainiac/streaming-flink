#!/bin/sh

cd /Users/mdaeppen/infra/kafka_2.12-2.6.0 || exit

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 5 --partitions 3 --config min.insync.replicas=4 --config unclean.leader.election.enable=true --topic TrafficIOTRaw
bin/kafka-topics.sh --create --bootstrap-server localhost:9093 --replication-factor 5 --partitions 10 --config min.insync.replicas=4 --config unclean.leader.election.enable=true --topic TrafficCounterRaw

echo "start kafka cruise control done"