package producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.*;

/**
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath streaming-flink-0.4.1.0.jar producer.KafkaProducerIOTSensorAnomaly localhost:9092
 *
 * @author Marcel Daeppen
 * @version 2021/06/06 17:14
 */

public class KafkaProducerIOTSensorAnomaly {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerIOTSensorAnomaly.class);
    private static final Random random = new SecureRandom();
    private static final String LOGGERMSG = "Program prop set {}";

    private static String brokerURI = "localhost:9092";
    private static long sleeptime = 1;

    public static void main(String[] args) throws Exception {

        if (args.length == 1) {
            brokerURI = args[0];
            String parm = "'use customized URI' = " + brokerURI + " & 'use default sleeptime' = " + sleeptime;
            LOG.info(LOGGERMSG, parm);
        } else if (args.length == 2) {
            brokerURI = args[0];
            setsleeptime(Long.parseLong(args[1]));
            String parm = "'use customized URI' = " + brokerURI + " & 'use customized sleeptime' = " + sleeptime;
            LOG.info(LOGGERMSG, parm);
        } else {
            String parm = "'use default URI' = " + brokerURI + " & 'use default sleeptime' = " + sleeptime;
            LOG.info(LOGGERMSG, parm);
        }

        //create kafka producer
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-Kafka-IOT-Sensor-Anomaly");
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");

        try (Producer<String, byte[]> producer = new KafkaProducer<>(config)) {

            WeightedRandomBag<Integer> itemDrops = new WeightedRandomBag<>();
            itemDrops.addEntry(0, 1.0);
            itemDrops.addEntry(1, 999.0);

            //prepare the record

            for (int i = 0; i < 1000000; i++) {
                Integer part = itemDrops.getRandom();
                String key = UUID.randomUUID().toString();

                if (part == 1){
                    ObjectNode messageJsonObject = jsonObject();
                    byte[] valueJson = objectMapper.writeValueAsBytes(messageJsonObject);
                    ProducerRecord<String, byte[]> eventrecord = new ProducerRecord<>("iot", key, valueJson);
                    RecordMetadata msg = producer.send(eventrecord).get();
                    LOG.info(new StringBuilder().append("Published ").append(msg.topic()).append("/").append(msg.partition()).append("/").append(msg.offset()).append(" (key=").append(key).append(") : ").append(messageJsonObject).toString());
                }
                else{
                    ObjectNode messageJsonObject = jsonObjectAnomaly();
                    byte[] valueJson = objectMapper.writeValueAsBytes(messageJsonObject);
                    ProducerRecord<String, byte[]> eventrecord = new ProducerRecord<>("iot", key, valueJson);
                    RecordMetadata msg = producer.send(eventrecord).get();
                    LOG.info(new StringBuilder().append("Published ").append(msg.topic()).append("/").append(msg.partition()).append("/").append(msg.offset()).append(" (key=").append(key).append(") : ").append(jsonObjectAnomaly()).toString());
                }


                // wait
                Thread.sleep(sleeptime);
            }
        }
    }

    // build random json object
    private static ObjectNode jsonObject() {

        ObjectNode report = objectMapper.createObjectNode();
        report.put("sensor_ts", Instant.now().toEpochMilli());
        report.put("sensor_id", (random.nextInt(41)));
        report.put("sensor_0", (random.nextInt(9)));
        report.put("sensor_1", (random.nextInt(11)));
        report.put("sensor_2", (random.nextInt(22)));
        report.put("sensor_3", (random.nextInt(33)));
        report.put("sensor_4", (random.nextInt(44)));
        report.put("sensor_5", (random.nextInt(55)));
        report.put("sensor_6", (random.nextInt(66)));
        report.put("sensor_7", (random.nextInt(77)));
        report.put("sensor_8", (random.nextInt(88)));
        report.put("sensor_9", (random.nextInt(99)));
        report.put("sensor_10", (random.nextInt(1010)));
        report.put("sensor_11", (random.nextInt(1111)));

        return report;
    }

    private static ObjectNode jsonObjectAnomaly() {

        ObjectNode report = objectMapper.createObjectNode();
        report.put("sensor_ts", Instant.now().toEpochMilli());
        report.put("sensor_id", (random.nextInt(3)));
        report.put("sensor_0", (random.nextInt(9)) + 99);
        report.put("sensor_1", (random.nextInt(11)) * 11);
        report.put("sensor_2", (random.nextInt(22)));
        report.put("sensor_3", (random.nextInt(33)));
        report.put("sensor_4", (random.nextInt(44)));
        report.put("sensor_5", (random.nextInt(55)));
        report.put("sensor_6", (random.nextInt(66)));
        report.put("sensor_7", (random.nextInt(77)));
        report.put("sensor_8", (random.nextInt(88)));
        report.put("sensor_9", (random.nextInt(99)));
        report.put("sensor_10", (random.nextInt(1010)));
        report.put("sensor_11", (random.nextInt(1111)));

        return report;
    }

    public static void setsleeptime(long sleeptime) {
        KafkaProducerIOTSensorAnomaly.sleeptime = sleeptime;
    }

    private static class WeightedRandomBag<T> {

        private final List<Entry> entries = new ArrayList<>();
        private final Random rand = new SecureRandom();
        private double accumulatedWeight;

        public void addEntry(T object, double weight) {
            accumulatedWeight += weight;
            Entry e = new Entry();
            e.object = object;
            e.accumulatedWeight = accumulatedWeight;
            entries.add(e);
        }

        public T getRandom() {
            double r = rand.nextDouble() * accumulatedWeight;

            for (Entry entry : entries) {
                if (entry.accumulatedWeight >= r) {
                    return entry.object;
                }
            }
            return null; //should only happen when there are no entries
        }

        private class Entry {
            double accumulatedWeight;
            T object;
        }
    }

}