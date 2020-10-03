#!/bin/sh
rm *.pid
rm streaming-flink*
rm nohup.out
rm -rf paho*
echo "clean-up done"

sudo apt-get update
sudo apt-get install mosquitto mosquitto-clients -y
sudo wget https://github.com/zBrainiac/streaming-flink/releases/download/0.3.0/streaming-flink-0.3.0.0.jar
sudo systemctl start mosquitto
echo "setup done"
cd
./minifi-0.6.0.1.2.0.0-70/bin/minifi.sh start &
echo $! > run_minifi.pid &
nohup java -classpath streaming-flink-0.3.0.0.jar producer.MqTTTrafficCollector tcp://localhost:1883 999 &
echo $! > run_MqTTTrafficCollector.pid &
nohup java -classpath streaming-flink-0.3.0.0.jar producer.MqTTTrafficIOTSensor tcp://localhost:1883 999 &
echo $! > run_MqTTTrafficIOTSensor.pid &