#!/bin/sh
# sudo chmod +x nohup.sh

rm *.pid
rm streaming-flink*
rm nohup.out
rm -rf paho*
echo "clean-up done"

sudo apt-get update
sudo apt-get install mosquitto mosquitto-clients -y
sudo systemctl start mosquitto

sudo wget https://github.com/zBrainiac/streaming-flink/releases/download/0.4.1/streaming-flink-0.4.1.0.jar

echo "-- Download and install MQTT Processor NAR file"
sudo retry_if_needed 5 5 "wget --progress=dot:giga https://repo1.maven.org/maven2/org/apache/nifi/nifi-mqtt-nar/1.8.0/nifi-mqtt-nar-1.8.0.nar -P /home/pi/minifi-0.6.0.1.2.1.0-23/lib"
sudo chown root:root /opt/cloudera/cem/minifi/lib/nifi-mqtt-nar-1.8.0.nar
sudo chmod 660 /opt/cloudera/cem/minifi/lib/nifi-mqtt-nar-1.8.0.nar

echo "-- Download and install Kafka Processor NAR file"
sudo retry_if_needed 5 5 "wget --progress=dot:giga https://repo1.maven.org/maven2/org/apache/nifi/nifi-kafka-2-0-nar/1.8.0/nifi-kafka-2-0-nar-1.8.0.nar -P /home/pi/minifi-0.6.0.1.2.1.0-23/lib"
sudo chown pi:pi /opt/cloudera/cem/minifi/lib/nifi-kafka-2-0-nar-1.8.0.nar
sudo chmod 660 /opt/cloudera/cem/minifi/lib/nifi-kafka-2-0-nar-1.8.0.nar

echo "setup done"
cd
./minifi-0.6.0.1.2.0.0-70/bin/minifi.sh start &
echo $! > run_minifi.pid &
nohup java -classpath streaming-flink-0.4.1.0.jar producer.MqTTTrafficCollector tcp://localhost:1883 999 &
echo $! > run_MqTTTrafficCollector.pid &
nohup java -classpath streaming-flink-0.4.1.0.jar producer.MqTTTrafficIOTSensor tcp://localhost:1883 999 &
echo $! > run_MqTTTrafficIOTSensor.pid &