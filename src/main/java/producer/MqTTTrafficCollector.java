package producer;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static java.util.Collections.unmodifiableList;


/**
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath streaming-flink-0.2-SNAPSHOT.jar producer.MqTTTrafficIOTSensor tcp://localhost:1883
 *
 * output:
 *   {"sensor_ts":1596956979295,"sensor_id":8,"probability":50,"sensor_x":47,"typ":"LKW","light":false,"license_plate":"DE 483-5849","toll_typ":"10-day"}
 *   {"sensor_ts":1596952895018,"sensor_id":10,"probability":52,"sensor_x":14,"typ":"Bike"}
 *
 * @author Marcel Daeppen
 * @version 2020/09/02 09:25
 */

public class MqTTTrafficCollector {
    private static final Random random = new Random();

    private static String brokerURI = "tcp://localhost:1883";
    private static long sleeptime;

    private static final List<String> license_plate_country = unmodifiableList(Arrays.asList(
            "AT", "CH", "DE"));

    private static final List<String> toll_typ = unmodifiableList(Arrays.asList(
            "none", "10-day", "2-month", "Annual"));

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
                    int s = random.nextInt(3);
                    switch (s) {
                        case 0:
                            message.setPayload(("{"
                                    + "\"sensor_ts\"" + ":" + Instant.now().toEpochMilli()
                                    + "," + "\"sensor_id\"" + ":" + random.nextInt(11)
                                    + "," + "\"probability\"" + ":" + random.nextInt(49) + 50
                                    + "," + "\"sensor_x\"" + ":" + random.nextInt(11)
                                    + "," + "\"typ\"" + ":" + "\"" + "Bike" + "\""
                                    + "," + "\"light\"" + ":" + random.nextBoolean()
                                    + "," + "\"license_plate\"" + ":" + "\"" + "n/a" + "\""
                                    + "," + "\"toll_typ\"" + ":" + "\"" + "n/a" + "\""
                                    + "}").getBytes());
                            break;
                        case 1:
                            message.setPayload(("{"
                                    + "\"sensor_ts\"" + ":" + Instant.now().toEpochMilli()
                                    + "," + "\"sensor_id\"" + ":" + random.nextInt(11)
                                    + "," + "\"probability\"" + ":" + random.nextInt(49) + 50
                                    + "," + "\"sensor_x\"" + ":" + random.nextInt(11)
                                    + "," + "\"typ\"" + ":" + "\"" + "LKW" + "\""
                                    + "," + "\"light\"" + ":" + random.nextBoolean()
                                    + "," + "\"license_plate\"" + ":" + "\"" + license_plate_country.get(random.nextInt(license_plate_country.size())) +" "+ (random.nextInt(998 + 1 - 50) + 50) + "-" + (random.nextInt(8999) + 1000) + "\""
                                    + "," + "\"toll_typ\"" + ":" + "\"" + toll_typ.get(random.nextInt(toll_typ.size())) + "\""
                                    + "}").getBytes());
                            break;
                        case 2:
                            message.setPayload(("{"
                                    + "\"sensor_ts\"" + ":" + Instant.now().toEpochMilli()
                                    + "," + "\"sensor_id\"" + ":" + random.nextInt(11)
                                    + "," + "\"probability\"" + ":" + random.nextInt(49) + 50
                                    + "," + "\"sensor_x\"" + ":" + random.nextInt(11)
                                    + "," + "\"typ\"" + ":" + "\"" + "PKW" + "\""
                                    + "," + "\"light\"" + ":" + random.nextBoolean()
                                    + "," + "\"license_plate\"" + ":" + "\"" + license_plate_country.get(random.nextInt(license_plate_country.size())) +" "+ (random.nextInt(998 + 1 - 50) + 50) + "-" + (random.nextInt(8999) + 1000) + "\""
                                    + "," + "\"toll_typ\"" + ":" + "\"" + toll_typ.get(random.nextInt(toll_typ.size())) + "\""
                                    + "}").getBytes());
                            break;
                        default:
                            System.err.println("i out of range");
                    }

                    client.publish("TrafficCounterRaw", message);
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
        MqTTTrafficCollector.sleeptime = sleeptime;
    }
}