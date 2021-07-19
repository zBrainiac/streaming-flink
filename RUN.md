## run:
### Requirements:  
- local installation of the latest Apache Kafka (e.g. on infra/kafka_2.12-2.4.1)
- local (up-to-date) IDE such as Intellij IDEA

### Local Kafka Environment:  
```
cd infra/kafka_2.12-2.4.1  
bin/zookeeper-server-start.sh config/zookeeper.properties  
bin/kafka-server-start.sh config/server.properties  


./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic trx &&  
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic fx &&  
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092 &&  
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trx
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fx
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1
```

### Local execution Environment:  
```
cd streaming-flink 
java -classpath target/streaming-flink-0.4.1.0.jar producer.KafkaIOTSensorSimulator
java -classpath target/streaming-flink-0.4.1.0.jar consumer.IoTUC1CountEventsPerSensorId  
java -classpath target/streaming-flink-0.4.1.0.jar consumer.IoTUC2CountEventsPerSensorIdFilter
java -classpath target/streaming-flink-0.4.1.0.jar consumer.IoTUC3CountEventsPerSensorIdSplitter
```

### Download release:  
cd /opt/cloudera/parcels/FLINK  
sudo wget https://github.com/zBrainiac/streaming-flink/releases/download/0.4.1/streaming-flink-0.4.1.0.jar -P /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming

### Upload release: 
scp -i field.pem GoogleDrive/workspace/streaming-flink/target/streaming-flink-0.4.1.0.jar centos@52.59.200.19:/tmp  
sudo mv /tmp/streaming-flink-0.4.1.0.jar /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming 



## Test data gen:
### TRX
run:  
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaFSICreditCartTRX or  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaFSICreditCartTRX localhost:9092 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaFSICreditCartTRX edge2ai-1.dim.local:9092
```
sample trx output json:
```
{"timestamp":1565604610745,"shop_id":4,"shop_name":"Ums Eck","cc_type":"Visa","cc_id":"cc_id":"5130-2220-4900-6727","amount_orig":86.82,"fx":"EUR","fx_account":"CHF"}
```   
### KafkaFootballTicketsTRX
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaFootballTicketsTRX or  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaFootballTicketsTRX localhost:9092 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaFootballTicketsTRX edge2ai-1.dim.local:9092
```


### FX
run:  
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaFSIFXRates or  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaFSIFXRates localhost:9092 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaFSIFXRates edge2ai-1.dim.local:9092
```  
sample fx output json:
```
{"timestamp":1565604610729,"fx":"EUR","fx_rate":0.91}
```
### IOT Sensor
run:
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaIOTSensorSimulator or  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaIOTSensorSimulator localhost:9092 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaIOTSensorSimulator edge2ai-1.dim.local:9092
```
simulation of anomalies 1:1'000
```
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaProducerIOTSensorAnomaly edge2ai-1.dim.local:9092 100                           
```  
sample iot output json:
```
{"sensor_ts":1588330712878,"sensor_id":1,"sensor_0":88,"sensor_1":93,"sensor_2":31,"sensor_3":90,"sensor_4":75,"sensor_5":74,"sensor_6":58,"sensor_7":91,"sensor_8":10,"sensor_9":21,"sensor_10":66,"sensor_11":40}
```

### IOT 32k string Sensor
run:
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.4.1.0.jar producer.Kafka32kStringSimulator or  
java -classpath streaming-flink-0.4.1.0.jar producer.Kafka32kStringSimulator localhost:9092 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  
java -classpath streaming-flink-0.4.1.0.jar producer.Kafka32kStringSimulator edge2ai-1.dim.local:9092                           
```  
sample iot output json:
```
{"sensor_ts":1588330712878,"32kString":"JNJiGn6....."}
```



### IOT Simple CSV generator
run:
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaIOTSimpleCSVProducer or  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaIOTSimpleCSVProducer localhost:9092 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaIOTSimpleCSVProducer edge2ai-1.dim.local:9092  
```  
sample CSV output message:
```
1596953344830, 10, 9d02e657-80c9-4857-b18b-26b58f09ae6c, Test Message #25
```  

### IOT Simple KV generator
run:  
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaIOTSimpleKVProducer or  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaIOTSimpleKVProducer localhost:9092 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaIOTSimpleKVProducer edge2ai-1.dim.local:9092  
```  
sample KeyValue output message:
```
unixTime: 1596953939783, sensor_id: 1, id: ba292ff6-e4db-4776-b70e-2b49edfb6726, Test Message: bliblablub #33
```  

### Lookup Geolocation
run:
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaLookupGeoLocation or  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaLookupGeoLocation localhost:9092 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaLookupGeoLocation edge2ai-1.dim.local:9092  
```

### Lookup Weather Condition
run:
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaLookupWeatherCondition or  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaLookupWeatherCondition localhost:9092 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaLookupWeatherCondition edge2ai-1.dim.local:9092  
```


### OPC Sensor
run:  
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaOPCSimulator or  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaOPCSimulator localhost:9092 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaOPCSimulator edge2ai-1.dim.local:9092
```  
sample opc output json:
```
{"__time":"2020-05-01T11:01:04.818786Z","tagname":"Triangle4711","unit":"Hydrocracker","value":0.96354}
```  

### Traffic Counter
run  
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaTrafficCollector or  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaTrafficCollector localhost:9092 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaTrafficCollector edge2ai-1.dim.local:9092
```  
sample TrafficCounter output json:
```
{"sensor_ts":1596956979295,"sensor_id":8,"probability":50,"sensor_x":47,"typ":"LKW","light":false,"license_plate":"DE 483-5849","toll_typ":"10-day"}
{"sensor_ts":1596952895018,"sensor_id":10,"probability":52,"sensor_x":14,"typ":"Bike"}
```  
### Traffic IOT Sensor
for SQL Lookup use case move lookup CSV to:  
```
/tmp/lookupHeader.csv  
```
run generator:
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaTrafficIOTSensor or  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaTrafficIOTSensor localhost:9092 10 (= 10 sleep time in ms between the messages | default 1'000 ms)  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaTrafficIOTSensor edge2ai-1.dim.local:9092
```  
sample IOT Sensor output json:
```
{"sensor_ts":1597138335247,"sensor_id":5,"temp":10,"rain_level":2,"visibility_level":2}
```  

### Unbalanced Kafka Generator
This Kafka producer send events unbalanced to a kafka topic.  

create kafka topic:
```
cd /opt/cloudera/parcels/CDH  
./bin/kafka-topics --create --bootstrap-server edge2ai-1.dim.local:9092 --replication-factor 1 --partitions 5 --topic kafka_unbalanced &&
./bin/kafka-topics --list --bootstrap-server edge2ai-1.dim.local:9092 &&
./bin/kafka-topics --describe --bootstrap-server edge2ai-1.dim.local:9092 --topic kafka_unbalanced && 
./bin/kafka-console-consumer --bootstrap-server edge2ai-1.dim.local:9092 --topic kafka_unbalanced
```
run generator:
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaProducerUnbalanced edge2ai-1.dim.local:9092 99
```
### Simple Kafka Generator
Super simple Kafka producer  

create kafka topic:
```
cd /opt/cloudera/parcels/CDH  
./bin/kafka-topics --create --bootstrap-server edge2ai-1.dim.local:9092 --replication-factor 1 --partitions 5 --topic kafka_simple &&
./bin/kafka-topics --list --bootstrap-server edge2ai-1.dim.local:9092 &&
./bin/kafka-console-consumer --bootstrap-server edge2ai-1.dim.local:9092 --topic kafka_simple
```
run generator:
```
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaProducerSimple edge2ai-1.dim.local:9092 99
```

### Let run multiple JAVA processes in the background
Create a new nohup.sh with a list of jar's  

Sample:
```
#!/bin/sh
nohup java -classpath streaming-flink-0.4.1.0.jar producer.KafkaFSIFXRates edge2ai-1.dim.local:9092 &
nohup java -classpath streaming-flink-0.4.1.0.jar producer.KafkaFSICreditCartTRX edge2ai-1.dim.local:9092 &
nohup java -classpath streaming-flink-0.4.1.0.jar producer.KafkaIOTSensorSimulator edge2ai-1.dim.local:9092 &
```

```
#!/bin/sh
cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming  
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaTrafficCollector edge2ai-1.dim.local:9092 &
echo $! > run_KafkaTrafficCollector.pid &
java -classpath streaming-flink-0.4.1.0.jar producer.KafkaTrafficIOTSensor edge2ai-1.dim.local:9092 &
echo $! > run_KafkaTrafficIOTSensor.pid &
```

make nohup.sh script executable:  
```
sudo chmod +x nohup.sh
```



### cross-checking the kafka topic
run:  
```
cd /opt/cloudera/parcels/CDH  
./bin/kafka-topics --list --bootstrap-server edge2ai-1.dim.local:9092  
./bin/kafka-console-consumer --bootstrap-server edge2ai-1.dim.local:9092 --topic result_iot_Consumer_Count
```

## Run Flink Apps on yarn cluster:  
Change to th following directory:  
```
cd /opt/cloudera/parcels/FLINK  
```
### iot
```
./bin/flink run -m yarn-cluster -c consumer.IoTUC1CountEventsPerSensorId -ynm IoTConsumerCount lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.IoTUC2CountEventsPerSensorIdFilter -ynm IoTConsumerFilter lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.IoTUC3CountEventsPerSensorIdSplitter -ynm IoTConsumerSplitter lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.IoTUC4JoinStreams -ynm IoTUC4JoinStreams lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092   
./bin/flink run -m yarn-cluster -c consumer.IoTUC5ConsumerCSVSQLFilter -ynm IoTUC5ConsumerCSVSQLFilter lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.IoTUC7ConsumerCSVSQLLookupCSV -ynm IoTCsvConsumerSQLLookupCSV lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.IoTUC6ConsumerCSVSQLLookupJSON -ynm IoTCsvConsumerSQLLookupJSON lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092 
```
### OPC
```
./bin/flink run -m yarn-cluster -c consumer.OPCUC1NoiseCanceller -ynm OPCNoiseCanceller lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092  
```
### FSI
```
./bin/flink run -m yarn-cluster -c consumer.FSIUC1KafkaCountTrxPerShop -ynm FSIUC1KafkaCountTrxPerShop lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.FSIUC2KafkaSumccTypTrxFx -ynm FSIUC2KafkaSumccTypTrxFx lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.FSIUC3KafkaJoin2JsonStreams -ynm FSIUC3KafkaJoin2JsonStreams lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar  edge2ai-1.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.FSIUC4KafkaJoin2JsonStreamsdiffOut -ynm FSIUC4KafkaJoin2JsonStreamsdiffOut lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar  edge2ai-1.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.FSIUC5KafkaTrxDuplicateChecker -ynm FSIUC5KafkaTrxDuplicateChecker lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.FSIUC6KafkaccTrxFraud -ynm FSIUC6KafkaccTrxFraud lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.FSIUC7KafkaAvgFx -ynm FSIUC7KafkaAvgFx lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092  
./bin/flink run -m yarn-cluster -c consumer.FSIUC8KafkaTRXAmountDispatcher -ynm FSIUC8KafkaTRXAmountDispatcher lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092
./bin/flink run -m yarn-cluster -c consumer.FSIUC9KafkalookupJson -ynm FSIUC9KafkalookupJson lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092 /tmp/lookup.csv

```

### Traffic
```
./bin/flink run -m yarn-cluster -c consumer.TrafficUC1CountSensorTyp -ynm TrafficUC1CountSensorTyp lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092
./bin/flink run -m yarn-cluster -c consumer.TrafficUC2WindowSensorTyp -ynm TrafficUC2WindowSensorTyp lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092
./bin/flink run -m yarn-cluster -c consumer.TrafficUC3SummeryPerSensor -ynm TrafficUC3SummeryPerSensor lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092
./bin/flink run -m yarn-cluster -c consumer.TrafficUC4TollValidation -ynm TrafficUC4TollValidation lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092
./bin/flink run -m yarn-cluster -c consumer.TrafficUC5Join -ynm TrafficUC5Join lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092
./bin/flink run -m yarn-cluster -c consumer.TrafficUC6SQLLookupJSON -ynm TrafficUC6SQLLookupJSON lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092 /tmp/lookupHeader.csv
./bin/flink run -m yarn-cluster -c consumer.TrafficUC7EventDispatcher -ynm TrafficUC7EventDispatcher lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar edge2ai-1.dim.local:9092
```

## Run Atlas sync:
run:
```
cd /opt/cloudera/parcels/CDH/lib/atlas/hook-bin  
./import-kafka.sh 
```