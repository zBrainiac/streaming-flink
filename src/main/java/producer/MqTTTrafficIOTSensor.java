package producer;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.time.Instant;
import java.util.Random;


/**
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath streaming-flink-0.2-SNAPSHOT.jar producer.MqTTTrafficIOTSensor tcp://localhost:1883
 *
 * output:
 * {"sensor_ts":1598712320504,"sensor_id":10,"temp":16,"rain_level":0,"visibility_level":2}
 *
 * @author Marcel Daeppen
 * @version 2020/08/29 16:25
 */

public class MqTTTrafficIOTSensor {
    private static final Random random = new Random();

    private static String brokerURI = "tcp://localhost:1883";
    private static long sleeptime;

    public static void main(String[] args) throws Exception {

        if (args.length == 1) {
            System.err.println("case 'customized URI':");
            brokerURI = args[0];
            System.err.println("arg URL: " + brokerURI);
            System.err.println("default sleeptime (ms): " + sleeptime);
        } else if (args.length == 2) {
            System.err.println("case 'customized URI & time':");
            brokerURI = args[0];
            setsleeptime(Long.parseLong(args[1]));
            System.err.println("arg URL: " + brokerURI);
            System.err.println("sleeptime (ms): " + sleeptime);
        } else {
            System.err.println("case default");
            System.err.println("default URI: " + brokerURI);
            setsleeptime(1000);
            System.err.println("default sleeptime (ms): " + sleeptime);
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

                    client.publish("iot", message);
                    System.out.println("Published data: " + message);

                    Thread.sleep(sleeptime);
                }
                client.disconnect();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void setsleeptime(long sleeptime) {
        MqTTTrafficIOTSensor.sleeptime = sleeptime;
    }
}