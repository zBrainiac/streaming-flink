package producer;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static java.util.Collections.unmodifiableList;


/**
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath streaming-flink-0.3.1.0.jar producer.MqTTTrafficCollector tcp://localhost:1883
 *
 * output:
 *   {"sensor_ts":1596956979295,"sensor_id":8,"probability":50,"sensor_x":47,"typ":"LKW","light":false,"license_plate":"DE 483-5849","toll_typ":"10-day"}
 *   {"sensor_ts":1596952895018,"sensor_id":10,"probability":52,"sensor_x":14,"typ":"Bike"}
 *
 * @author Marcel Daeppen
 * @version 2020/09/02 09:25
 */

public class MqTTTrafficCollector {

    private static final Logger LOG = LoggerFactory.getLogger(MqTTTrafficCollector.class);
    private static final Random random = new SecureRandom();
    private static String brokerURI = "tcp://localhost:1883";
    private static final String LOGGERMSG = "Program prop set {}";
    private static long sleeptime = 1000;

    private static final List<String> license_plate_country = unmodifiableList(Arrays.asList(
            "AT", "CH", "DE"));

    private static final List<String> toll_duration = unmodifiableList(Arrays.asList(
            "none", "10-day", "2-month", "Annual"));

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

                WeightedRandomBag<Integer> itemTyp = new WeightedRandomBag<>();
                itemTyp.addEntry(0, 25.0);
                itemTyp.addEntry(1, 20.0);
                itemTyp.addEntry(2, 55.0);

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
                    int s = itemTyp.getRandom();

                    String sensor_ts = "\"sensor_ts\"" ;
                    String sensor_id = "\"sensor_id\"" ;
                    String probability = "\"probability\"" ;
                    String sensor_x = "\"sensor_x\"" ;
                    String typ = "\"typ\"" ;
                    String light = "\"light\"" ;
                    String license_plate = "\"license_plate\"" ;
                    String toll_typ = "\"toll_typ\"";

                    switch (s) {
                        case 0:
                            message.setPayload(("{"
                                    + sensor_ts + ":" + Instant.now().toEpochMilli()
                                    + "," + sensor_id + ":" + sensorDrops.getRandom()
                                    + "," + probability + ":" + random.nextInt(49) + 50
                                    + "," + sensor_x + ":" + random.nextInt(11)
                                    + "," + typ + ":" + "\"" + "Bike" + "\""
                                    + "," + light + ":" + random.nextBoolean()
                                    + "," + license_plate + ":" + "\"" + "n/a" + "\""
                                    + "," + toll_typ + ":" + "\"" + "n/a" + "\""
                                    + "}").getBytes());
                            break;
                        case 1:
                            message.setPayload(("{"
                                    + sensor_ts + ":" + Instant.now().toEpochMilli()
                                    + "," + sensor_id + ":" + sensorDrops.getRandom()
                                    + "," + probability + ":" + random.nextInt(49) + 50
                                    + "," + sensor_x + ":" + random.nextInt(11)
                                    + "," + typ + ":" + "\"" + "LKW" + "\""
                                    + "," + light + ":" + random.nextBoolean()
                                    + "," + license_plate + ":" + "\"" + license_plate_country.get(random.nextInt(license_plate_country.size())) +" "+ (random.nextInt(998 + 1 - 50) + 50) + "-" + (random.nextInt(8999) + 1000) + "\""
                                    + "," + toll_typ + ":" + "\"" + toll_duration.get(random.nextInt(toll_duration.size())) + "\""
                                    + "}").getBytes());
                            break;
                        case 2:
                            message.setPayload(("{"
                                    + sensor_ts + ":" + Instant.now().toEpochMilli()
                                    + "," + sensor_id + ":" + sensorDrops.getRandom()
                                    + "," + probability + ":" + random.nextInt(49) + 50
                                    + "," + sensor_x + ":" + random.nextInt(11)
                                    + "," + typ + ":" + "\"" + "PKW" + "\""
                                    + "," + light + ":" + random.nextBoolean()
                                    + "," + license_plate + ":" + "\"" + license_plate_country.get(random.nextInt(license_plate_country.size())) +" "+ (random.nextInt(998 + 1 - 50) + 50) + "-" + (random.nextInt(8999) + 1000) + "\""
                                    + "," + toll_typ + ":" + "\"" + toll_duration.get(random.nextInt(toll_duration.size())) + "\""
                                    + "}").getBytes());
                            break;
                        default:
                            System.err.println("i out of range");
                    }

                    client.publish("TrafficCounterRaw", message);
                    LOG.info("Published data: {}", message);

                    Thread.sleep(sleeptime);
                }
                client.disconnect();
            }

        } catch (Exception e) {
            LOG.info("Exception message: ", e);
        }
    }

    private static class WeightedRandomBag<T> {

        private class Entry {
            double accumulatedWeight;
            T object;
        }

        private final List<Entry> entries = new ArrayList<>();
        private double accumulatedWeight;
        private final Random rand = new SecureRandom();

        public void addEntry(T object, double weight) {
            accumulatedWeight += weight;
            Entry e = new Entry();
            e.object = object;
            e.accumulatedWeight = accumulatedWeight;
            entries.add(e);
        }

        public T getRandom() {
            double r = rand.nextDouble() * accumulatedWeight;

            return entries.stream().filter(entry -> entry.accumulatedWeight >= r).findFirst().map(entry -> entry.object).orElse(null);
            //should only happen when there are no entries
        }
    }
    public static void setsleeptime(long sleeptime) {
        MqTTTrafficCollector.sleeptime = sleeptime;
    }
}