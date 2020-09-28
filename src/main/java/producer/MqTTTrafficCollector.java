package producer;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.time.Instant;
import java.util.ArrayList;
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

                WeightedRandomBag<Integer> itemTyp = new WeightedRandomBag<>();
                itemTyp.addEntry(0, 25.0);
                itemTyp.addEntry(1, 20.0);
                itemTyp.addEntry(2, 55.0);

                WeightedRandomBag<Integer> itemDrops = new WeightedRandomBag<>();
                itemDrops.addEntry(0, 25.0);
                itemDrops.addEntry(1, 20.0);
                itemDrops.addEntry(2, 55.0);



                WeightedRandomBag<Integer> sensorDrops = new WeightedRandomBag<>();
                sensorDrops.addEntry(0, 5.0);
                sensorDrops.addEntry(1, 20.0);
                sensorDrops.addEntry(2, 15.0);
                sensorDrops.addEntry(3, 15.0);
                sensorDrops.addEntry(4, 10.0);
                sensorDrops.addEntry(5, 5.0);
                sensorDrops.addEntry(6, 5.0);
                sensorDrops.addEntry(7, 15.0);
                sensorDrops.addEntry(8, 5.0);
                sensorDrops.addEntry(9, 5.0);



                for (int i = 0; i < 1000000; i++) {
                    MqttMessage message = new MqttMessage();
                    int s = itemDrops.getRandom();
                    switch (s) {
                        case 0:
                            message.setPayload(("{"
                                    + "\"sensor_ts\"" + ":" + Instant.now().toEpochMilli()
                                    + "," + "\"sensor_id\"" + ":" + sensorDrops.getRandom()
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
                                    + "," + "\"sensor_id\"" + ":" + sensorDrops.getRandom()
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
                                    + "," + "\"sensor_id\"" + ":" + sensorDrops.getRandom()
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

    private static class WeightedRandomBag<T> {

        private class Entry {
            double accumulatedWeight;
            T object;
        }

        private final List<WeightedRandomBag.Entry> entries = new ArrayList<>();
        private double accumulatedWeight;
        private final Random rand = new Random();

        public void addEntry(T object, double weight) {
            accumulatedWeight += weight;
            WeightedRandomBag.Entry e = new WeightedRandomBag.Entry();
            e.object = object;
            e.accumulatedWeight = accumulatedWeight;
            entries.add(e);
        }

        public T getRandom() {
            double r = rand.nextDouble() * accumulatedWeight;

            for (WeightedRandomBag.Entry entry: entries) {
                if (entry.accumulatedWeight >= r) {
                    return (T) entry.object;
                }
            }
            return null; //should only happen when there are no entries
        }
    }
    public static void setsleeptime(long sleeptime) {
        MqTTTrafficCollector.sleeptime = sleeptime;
    }
}