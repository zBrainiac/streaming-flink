#!/bin/sh
nohup java -classpath streaming-flink-0.2-SNAPSHOT.jar producer.KafkaJsonProducer_fx edge2ai-1.dim.local:9092 &
nohup java -classpath streaming-flink-0.2-SNAPSHOT.jar producer.KafkaJsonProducer_trx edge2ai-1.dim.local:9092 &
nohup java -classpath streaming-flink-0.2-SNAPSHOT.jar producer.KafkaIOTSensorSimulator edge2ai-1.dim.local:9092 &