# Run MiNiFi on a Raspberry:
## Requirements:  
- Raspberry pi 3 b+


## EFM: 
### EFM setup on mac:  
Install mysql db (v5.7)
```
mysql -u root -p
mysql> CREATE DATABASE efm CHARACTER SET latin1;
mysql> CREATE USER 'efm'@'%' IDENTIFIED BY 'efmPassword';
mysql> GRANT ALL PRIVILEGES ON efm.* TO 'efm'@'%';
mysql> FLUSH PRIVILEGES;

```

update efm.Properties
```
# Database Properties
#efm.db.url=jdbc:h2:./database/efm;AUTOCOMMIT=OFF;DB_CLOSE_ON_EXIT=FALSE;LOCK_MODE=3
#efm.db.driverClass=org.h2.Driver
efm.db.url=jdbc:mysql://localhost:3306/efm
efm.db.driverClass=com.mysql.jdbc.Driver
efm.db.username=efm
efm.db.password=efmPassword
```

### EFM start:  
```
cd /Users/mdaeppen/infra/efm-1.0.0.1.2.1.0-23
./efm/efm.sh start --efm.encryption.password=setAnEncryptionPasswordHere

```
Link to: [efm designer](http://localhost:10080/efm/ui/)



## IoT Agent: 
### Download release:  
```
cd /opt/cloudera/parcels/FLINK  
sudo wget https://github.com/zBrainiac/streaming-flink/releases/download/0.3.0/streaming-flink-0.3.1.0.jar -P /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming

java -classpath streaming-flink-0.3.1.0.jar producer.MqTTTrafficIOTSensor tcp://18.184.199.92:1883 999
java -classpath streaming-flink-0.3.1.0.jar producer.MqTTTrafficCollector tcp://18.184.199.92:1883 999

cd /tmp
sudo  wget https://raw.githubusercontent.com/zBrainiac/streaming-flink/master/data/lookupHeader.csv -P /tmp
sudo  wget https://raw.githubusercontent.com/zBrainiac/streaming-flink/master/data/geolocation_ru.csv -P /tmp
sudo  wget https://raw.githubusercontent.com/zBrainiac/streaming-flink/master/data/geolocation_de.csv -P /tmp
sudo  wget https://raw.githubusercontent.com/zBrainiac/streaming-flink/master/data/geolocation_ch.csv -P /tmp
sudo  wget https://raw.githubusercontent.com/zBrainiac/streaming-flink/master/data/geolocation_at.csv -P /tmp
```
### Upload latest MiNiFi release: 
```
scp -i "field.pem" /Users/mdaeppen/infra/minifi-0.6.0.1.2.1.0-23-bin.tar.gz centos@ec2-3-122-237-145.eu-central-1.compute.amazonaws.com:/home/centos
tar -xvf minifi-0.6.0.1.2.1.0-23-bin.tar.gz 
cd minifi-0.6.0.1.2.1.0-23
```

## MiNiFi (e.g running on a Raspberry Pi):  
### MiNiFi setup:  


config/bootstrap.conf:  
```
nifi.c2.enable=true
## define protocol parameters
nifi.c2.rest.url=http://192.168.0.87:10080/efm/api/c2-protocol/heartbeat
nifi.c2.rest.url.ack=http://192.168.0.87:10080/efm/api/c2-protocol/acknowledge
## heartbeat in milliseconds.  defaults to once a second
nifi.c2.agent.heartbeat.period=1000
## define parameters about your agent
nifi.c2.agent.class=iot-1-0001
# Optional.  Defaults to a hardware based unique identifier
nifi.c2.agent.identifier=agemt-traffic-0001
```
### MiNiFi start:  

```
cd /Users/mdaeppen/infra/minifi-0.6.0.1.2.1.0-23
./bin/minifi.sh start
```


## NiFi: 
### NiFi setup:  

config/nifi.properties  
```
# Site to Site properties
nifi.remote.input.host=localhost
nifi.remote.input.secure=false
nifi.remote.input.socket.port=10000
nifi.remote.input.http.enabled=true
nifi.remote.input.http.transaction.ttl=30 sec
nifi.remote.contents.cache.expiration=30 secs
```
### NiFi start:  
```
./bin/nifi.sh start
```

### Let run multiple JAVA processes in the background
Create a new nohup.sh with a list of jar's  
+ Sample: [link](nohup_minifi.sh)

Make nohup.sh script executable:  
```
sudo chmod +x nohup_minifi.sh
```


## run local: 

cd /Users/mdaeppen/infra/kafka_2.12-2.4.1  
./bin/zookeeper-server-start.sh config/zookeeper.properties  
./bin/kafka-server-start.sh config/server.properties  
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic minifi  
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092  
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic minifi  
{"filename": "minifi-rel0001.txt","version": "0.0.0.1"}  



./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic agent_log_minifi-bootstrap   
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic minifi-ack  


## SQL on Impala/Kudu:

```
SELECT 	sensor_id, temp, city, count(*)  FROM sensors_enhanced
GROUP BY sensor_id, temp, city;
```

