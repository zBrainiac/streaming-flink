#!/bin/sh

echo "$1"
if [ "$1" = "-c" ]; then
  cd || exit
  cd infra/kafka_2.12-2.6.0 || exit

  var=$(cat run_KafkaServer1.pid)
  echo "${var}"
  kill -9  "${var}"
  rm run_KafkaServer1.pid
  rm run_KafkaServer1.log

  var=$(cat run_KafkaServer2.pid)
  echo "${var}"
  kill -9  "${var}"
  rm run_KafkaServer2.pid
  rm run_KafkaServer2.log

  var=$(cat run_KafkaServer3.pid)
  echo "${var}"
  kill -9  "${var}"
  rm run_KafkaServer3.pid
  rm run_KafkaServer3.log

  var=$(cat run_zookeeper1.pid)
  echo "${var}"
  kill -9  "${var}"
  rm run_zookeeper1.pid
  rm run_zookeeper1.log

  var=$(cat run_zookeeper2.pid)
  echo "${var}"
  kill -9  "${var}"
  rm run_zookeeper2.pid
  rm run_zookeeper2.log

  var=$(cat run_zookeeper3.pid)
  echo "${var}"
  kill -9  "${var}"
  rm run_zookeeper3.pid
  rm run_zookeeper3.log

  echo "clean-up done"

elif [ "$1" = "-s" ]; then

  cd || exit

  cd infra/kafka_2.12-2.6.0 || exit

  nohup bin/zookeeper-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/zookeeper1.properties >run_zookeeper1.log &
  echo $! >run_zookeeper1.pid &
  nohup bin/zookeeper-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/zookeeper2.properties >run_zookeeper2.log &
  echo $! >run_zookeeper2.pid &
  nohup bin/zookeeper-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/zookeeper3.properties >run_zookeeper3.log &
  echo $! >run_zookeeper3.pid &
  echo "start zookeeper done"
  nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server1.properties >run_KafkaServer1.log &
  echo $! >run_KafkaServer1.pid &
  nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server2.properties >run_KafkaServer2.log &
  echo $! >run_KafkaServer2.pid &
  nohup bin/kafka-server-start.sh /Users/mdaeppen/GoogleDrive/workspace/streaming-flink/showcase/kafkaISR/config/server3.properties >run_KafkaServer3.log &
  echo $! >run_KafkaServer3.pid &

else
  echo -e "Optionen: \n -c = clean \n -s = start \n -w oder --which    zeigt die zur startenden VMs"
fi
