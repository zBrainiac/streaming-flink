#!/bin/sh
nohup java -classpath streaming-flink-0.5.0.0.jar producer.KafkaJsonProducer_fx edge2ai-0.dim.local:9092 &
echo $! > KafkaJsonProducer_fx.pid &
nohup java -classpath streaming-flink-0.5.0.0.jar producer.KafkaJsonProducer_trx edge2ai-0.dim.local:9092 &
echo $! > KafkaJsonProducer_trx.pid &
nohup java -classpath streaming-flink-0.5.0.0.jar producer.KafkaIOTSensorSimulator edge2ai-0.dim.local:9092 &
echo $! > KafkaIOTSensorSimulator.pid &