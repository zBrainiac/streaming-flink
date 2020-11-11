# 2020-09-09 Data Cafe

```
cd /opt/cloudera/parcels/CDH  
./bin/kafka-topics --create --bootstrap-server edge2ai-1.dim.local:9092 --replication-factor 1 --partitions 5 --topic kafka_unbalanced &&
./bin/kafka-topics --create --bootstrap-server edge2ai-1.dim.local:9092 --replication-factor 1 --partitions 5 --topic kafka_simple &&
./bin/kafka-topics --list --bootstrap-server edge2ai-1.dim.local:9092 &&
./bin/kafka-topics --describe --bootstrap-server edge2ai-1.dim.local:9092 --topic kafka_unbalanced

./bin/kafka-topics --delete --bootstrap-server edge2ai-1.dim.local:9092 --topic kafka_unbalanced 


cd /opt/cloudera/parcels/FLINK  
sudo wget https://github.com/zBrainiac/streaming-flink/releases/download/0.2.5.3/streaming-flink-0.3.0.1.jar -P /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming


cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.3.0.1.jar producer.KafkaProducerUnbalanced edge2ai-1.dim.local:9092
java -classpath streaming-flink-0.3.0.1.jar producer.KafkaProducerSimple edge2ai-1.dim.local:9092
java -classpath streaming-flink-0.3.0.1.jar producer.KafkaTrafficIOTSensor edge2ai-1.dim.local:9092
java -classpath streaming-flink-0.3.0.1.jar producer.KafkaTrafficCollector edge2ai-1.dim.local:9092



./bin/flink run -m yarn-cluster -c consumer.TrafficUC5Join -ynm TrafficUC5Join lib/flink/examples/streaming/streaming-flink-0.3.0.1.jar edge2ai-1.dim.local:9092

cd /opt/cloudera/parcels/CDH
./bin/kafka-topics --list --bootstrap-server edge2ai-1.dim.local:9092
./bin/kafka-topics --describe --bootstrap-server edge2ai-1.dim.local:9092 --topic kafka_unbalanced
./bin/kafka-topics --describe --bootstrap-server edge2ai-1.dim.local:9092 --topic kafka_simple



```