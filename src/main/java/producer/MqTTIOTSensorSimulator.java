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
 * {"sensor_ts":1606234000126,"sensor_id":76,"sensor_0":1,"sensor_1":27,"sensor_2":54,"sensor_3":54,"sensor_4":76,"sensor_5":68,"sensor_6":25,"sensor_7":19,"sensor_8":7,"sensor_9":18,"sensor_10":80,"sensor_11":93}
 *
 * @author Marcel Daeppen
 * @version 2020/11/24 17:05 UTC
 */

public class MqTTIOTSensorSimulator {

    private static final Logger LOG = LoggerFactory.getLogger(MqTTIOTSensorSimulator.class);
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
                            + "," + "\"sensor_id\"" + ":" + random.nextInt(101)
                            + "," + "\"sensor_0\"" + ":" + random.nextInt(42 - 20 + 1)
                            + "," + "\"sensor_1\"" + ":" + random.nextInt(99)
                            + "," + "\"sensor_2\"" + ":" + random.nextInt(99)
                            + "," + "\"sensor_3\"" + ":" + random.nextInt(99)
                            + "," + "\"sensor_4\"" + ":" + random.nextInt(99)
                            + "," + "\"sensor_5\"" + ":" + random.nextInt(99)
                            + "," + "\"sensor_6\"" + ":" + random.nextInt(99)
                            + "," + "\"sensor_7\"" + ":" + random.nextInt(99)
                            + "," + "\"sensor_8\"" + ":" + random.nextInt(99)
                            + "," + "\"sensor_9\"" + ":" + random.nextInt(99)
                            + "," + "\"sensor_10\"" + ":" + random.nextInt(99)
                            + "," + "\"sensor_11\"" + ":" + random.nextInt(99)
                            + "}").getBytes());

                    client.publish("iot", message);
                    LOG.info("Published data: {}", message);

                    Thread.sleep(sleeptime);
                }
                client.disconnect();
            }

        } catch (Exception e) {
            LOG.info("Exception message: ", e);
        }
    }

    public static void setsleeptime(long sleeptime) { MqTTIOTSensorSimulator.sleeptime = sleeptime;
    }
}