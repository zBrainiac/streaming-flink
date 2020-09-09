# Run sensor on a Raspberry pi 3 b+:
## Requirements:  
- Raspberry pi 3 b+

## MiNiFi (on Raspberry):  
### MiNiFi setup:  

config/bootstrap.conf:  
```
nifi.c2.enable=true
## define protocol parameters
nifi.c2.rest.url=http://192.168.0.10:10080/efm/api/c2-protocol/heartbeat
nifi.c2.rest.url.ack=http://192.168.0.10:10080/efm/api/c2-protocol/acknowledge
## heartbeat in milliseconds.  defaults to once a second
nifi.c2.agent.heartbeat.period=1000
## define parameters about your agent
nifi.c2.agent.class=iot-1
# Optional.  Defaults to a hardware based unique identifier
nifi.c2.agent.identifier=raspi-agent-iot-1
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
### NiFi setup:  
```
./bin/nifi.sh start
```

### Let run multiple JAVA processes in the background
Create a new nohup.sh with a list of jar's  
+ Sample: [link](nohup.sh)

Make nohup.sh script executable:  
```
sudo chmod +x nohup.sh
```



