# Run sensor on a Raspberry pi 3 b+:
## Requirements:  
- Raspberry pi 3 b+


### Download release:  
cd /opt/cloudera/parcels/FLINK  
sudo wget https://github.com/zBrainiac/streaming-flink/releases/download/0.3.0/streaming-flink-0.3.0.0.jar -P /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming


java -classpath streaming-flink-0.3.0.0.jar producer.MqTTTrafficIOTSensor tcp://18.184.199.92:1883 999
java -classpath streaming-flink-0.3.0.0.jar producer.MqTTTrafficCollector tcp://18.184.199.92:1883 999


```
scp -i "field.pem" /Users/mdaeppen/infra/minifi-0.6.0.1.2.1.0-23-bin.tar.gz centos@ec2-3-122-237-145.eu-central-1.compute.amazonaws.com:/home/centos  
```

## MiNiFi (on Raspberry):  
### MiNiFi setup:  


config/bootstrap.conf:  
```
nifi.c2.enable=true
## define protocol parameters
nifi.c2.rest.url=http://18.184.199.92:10080/efm/api/c2-protocol/heartbeat
nifi.c2.rest.url.ack=http://18.184.199.92:10080/efm/api/c2-protocol/acknowledge
## heartbeat in milliseconds.  defaults to once a second
nifi.c2.agent.heartbeat.period=1000
## define parameters about your agent
nifi.c2.agent.class=iot-2
# Optional.  Defaults to a hardware based unique identifier
nifi.c2.agent.identifier=traffic-4
```
### MiNiFi start:  

```
./bin/minifi.sh start
```

## EFM: 
### EFM setup:  

add to efm.properties
```
efm.encryption.password=setAnEncryptionPasswordHere
```
### EFM start:  
```
./bin/efm.sh start
```
> http://localhost:10080/efm/ui/

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