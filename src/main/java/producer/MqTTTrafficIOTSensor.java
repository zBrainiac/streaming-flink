package producer;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Random;


/**
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath streaming-flink-0.3.0.1.jar producer.MqTTTrafficIOTSensor tcp://localhost:1883
 *
 * output:
 * {"sensor_ts":1598712320504,"sensor_id":10,"temp":16,"rain_level":0,"visibility_level":2}
 *
 * @author Marcel Daeppen
 * @version 2020/08/29 16:25
 */

public class MqTTTrafficIOTSensor {

        private static final Logger LOG = LoggerFactory.getLogger(MqTTTrafficIOTSensor.class);
        private static final Random random = new SecureRandom();
        private static String brokerURI = "tcp://localhost:1883";
        private static final String LOGGERMSG = "Program prop set {}";
        private static long sleeptime = 1000;

        public static void main(String[] args) {

            if( args.length == 1 ) {
                brokerURI = args[0];
                String parm = "'use customized URI' = " + brokerURI + " & 'use default sleeptime' = " + sleeptime ;
                LOG.info(LOGGERMSG, parm);
            }else if( args.length == 2 ) {
                brokerURI = args[0];
                setsleeptime(Long.parseLong(args[1]));
                String parm = "'use customized URI' = " + brokerURI + " & 'use customized sleeptime' = " + sleeptime ;
                LOG.info(LOGGERMSG, parm);
            }else {
                String parm = "'use default URI' = " + brokerURI + " & 'use default sleeptime' = " + sleeptime ;
                LOG.info(LOGGERMSG, parm);
            }

        try  {
            try (MqttClient client = new MqttClient(brokerURI, MqttClient.generateClientId())) {
                client.connect();

                for (int i = 0; i < 1000000; i++) {
                    MqttMessage message = new MqttMessage();
                    message.setPayload(("{"
                            + "\"sensor_ts\"" + ":" + Instant.now().toEpochMilli()
                            + "," + "\"sensor_id\"" + ":" + random.nextInt(11)
                            + "," + "\"temp\"" + ":" + random.nextInt(42 - 20 + 1)
                            + "," + "\"rain_level\"" + ":" + random.nextInt(5)
                            + "," + "\"visibility_level\"" + ":" + random.nextInt(5)
                            + "}").getBytes());

                    client.publish("TrafficIOTRaw", message);
                    LOG.info("Published data: {}", message);

                    Thread.sleep(sleeptime);
                }
                client.disconnect();
            }

        } catch (Exception e) {
            LOG.info("Exception message: ", e);
        }
    }

    public static void setsleeptime(long sleeptime) {
        MqTTTrafficIOTSensor.sleeptime = sleeptime;
    }
}