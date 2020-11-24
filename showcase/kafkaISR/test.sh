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