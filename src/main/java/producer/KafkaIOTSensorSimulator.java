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
import java.util.Properties;
import java.util.Random;
import java.util.UUID;


/**
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath streaming-flink-0.4.0.0.jar producer.KafkaIOTSensorSimulator localhost:9092
 *
 * @author Marcel Daeppen
 * @version 2020/07/11 12:14
 */

public class KafkaIOTSensorSimulator {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(KafkaIOTSensorSimulator.class);
    private static final Random random = new SecureRandom();
    private static final String LOGGERMSG = "Program prop set {}";
    private static String brokerURI = "localhost:9092";
    private static long sleeptime = 1000;

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

        try (Producer<String, byte[]> producer = createProducer()) {
            for (int i = 0; i < 1000000; i++) {
                publishMessage(producer);
                Thread.sleep(sleeptime);
            }
        }
    }

    private static Producer<String, byte[]> createProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-IoT");
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");
        return new KafkaProducer<>(config);
    }

    private static void publishMessage(Producer<String, byte[]> producer) throws Exception {
        String key = UUID.randomUUID().toString();

        ObjectNode messageJsonObject = jsonObject();
        byte[] valueJson = objectMapper.writeValueAsBytes(messageJsonObject);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>("iot", key, valueJson);

        RecordMetadata msg = producer.send(record).get();

        System.err.println("Published " + msg.topic() + "/" + msg.partition() + "/" + msg.offset()
                + " (key=" + key + ") : " + messageJsonObject);

    }

    // build random json object
    private static ObjectNode jsonObject() {

        ObjectNode report = objectMapper.createObjectNode();
        report.put("sensor_ts", Instant.now().toEpochMilli());
        report.put("sensor_id", (random.nextInt(11)));
        report.put("sensor_0", (random.nextInt(99)));
        report.put("sensor_1", (random.nextInt(99)));
        report.put("sensor_2", (random.nextInt(99)));
        report.put("sensor_3", (random.nextInt(99)));
        report.put("sensor_4", (random.nextInt(99)));
        report.put("sensor_5", (random.nextInt(99)));
        report.put("sensor_6", (random.nextInt(99)));
        report.put("sensor_7", (random.nextInt(99)));
        report.put("sensor_8", (random.nextInt(99)));
        report.put("sensor_9", (random.nextInt(99)));
        report.put("sensor_10", (random.nextInt(99)));
        report.put("sensor_11", (random.nextInt(99)));

        return report;
    }

    public static void setsleeptime(long sleeptime) {
        KafkaIOTSensorSimulator.sleeptime = sleeptime;
    }
}