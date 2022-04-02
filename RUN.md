## run:
### Requirements:  
- local installation of the latest Apache Kafka (e.g. on infra/kafka_2.12-2.8.1)
- local (up-to-date) IDE such as Intellij IDEA

### Local Kafka Environment:  
```
cd infra/kafka_2.12-2.8.1  
bin/zookeeper-server-start.sh config/zookeeper.properties  
bin/kafka-server-start.sh config/server.properties  


./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic trx &&  
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic fx &&  
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092 &&  
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trx
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fx
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1
```


### Download release:  
cd /opt/cloudera/parcels/FLINK  
sudo wget https://github.com/zBrainiac/streaming-flink/releases/download/0.5.0/streaming-flink-0.5.0.0.jar -P /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming

### Upload release: 
scp -i field.pem GoogleDrive/workspace/streaming-flink/target/streaming-flink-0.5.0.0.jar centos@52.59.200.19:/tmp  
sudo mv /tmp/streaming-flink-0.5.0.0.jar /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming

# Test data gen:

### cross-checking the kafka topic
run:  
```
cd /opt/cloudera/parcels/CDH  
./bin/kafka-topics --list --bootstrap-server edge2ai-0.dim.local:9092  
./bin/kafka-console-consumer --bootstrap-server edge2ai-0.dim.local:9092 --topic result_iot_Consumer_Count
```

## Run Flink Apps on yarn cluster:  
Change to th following directory:  
```
cd /opt/cloudera/parcels/FLINK  
```
### iot
```
./bin/flink run -m yarn-cluster -c consumer.IoTUC1CountEventsPerSensorId -ynm IoTConsumerCount lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.IoTUC2CountEventsPerSensorIdFilter -ynm IoTConsumerFilter lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.IoTUC3CountEventsPerSensorIdSplitter -ynm IoTConsumerSplitter lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.IoTUC4JoinStreams -ynm IoTUC4JoinStreams lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092   
./bin/flink run -m yarn-cluster -c consumer.IoTUC5ConsumerCSVSQLFilter -ynm IoTUC5ConsumerCSVSQLFilter lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.IoTUC7ConsumerCSVSQLLookupCSV -ynm IoTCsvConsumerSQLLookupCSV lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.IoTUC6ConsumerCSVSQLLookupJSON -ynm IoTCsvConsumerSQLLookupJSON lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092 
```
### OPC
```
./bin/flink run -m yarn-cluster -c consumer.OPCUC1NoiseCanceller -ynm OPCNoiseCanceller lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092  
```
### FSI
```
./bin/flink run -m yarn-cluster -c consumer.FSIUC1KafkaCountTrxPerShop -ynm FSIUC1KafkaCountTrxPerShop lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.FSIUC2KafkaSumccTypTrxFx -ynm FSIUC2KafkaSumccTypTrxFx lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.FSIUC3Join2JsonStreams -ynm FSIUC3KafkaJoin2JsonStreams lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar  edge2ai-0.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.FSIUC4KafkaJoin2JsonStreamsdiffOut_old -ynm FSIUC4KafkaJoin2JsonStreamsdiffOut lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar  edge2ai-0.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.FSIUC5TrxDuplicateChecker -ynm FSIUC5KafkaTrxDuplicateChecker lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.FSIUC6CreditCartTrxFraud -ynm FSIUC6KafkaccTrxFraud lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.FSIUC8CCTRXAmountDispatcher -ynm FSIUC8KafkaTRXAmountDispatcher lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092

```