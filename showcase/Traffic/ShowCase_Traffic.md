# Run sensor on a Raspberry pi 3 b+:
## Requirements:  
- Raspberry pi 3 b+


## EFM: 
### EFM setup:  
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
./bin/efm.sh start --efm.encryption.password=setAnEncryptionPasswordHere

```
> http://localhost:10080/efm/ui/

## IoT Agent: 
### Download release:  
```
cd /opt/cloudera/parcels/FLINK  
sudo wget https://github.com/zBrainiac/streaming-flink/releases/download/0.3.0/streaming-flink-0.3.0.0.jar -P /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming

java -classpath streaming-flink-0.3.0.0.jar producer.MqTTTrafficIOTSensor tcp://18.184.199.92:1883 999
java -classpath streaming-flink-0.3.0.0.jar producer.MqTTTrafficCollector tcp://18.184.199.92:1883 999
```
### Upload latest MiNiFi release: 
```
scp -i "field.pem" /Users/mdaeppen/infra/minifi-0.6.0.1.2.1.0-23-bin.tar.gz centos@ec2-3-122-237-145.eu-central-1.compute.amazonaws.com:/home/centos  
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



CREATE DATABASE iot;