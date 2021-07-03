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
 *   cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 *   java -classpath streaming-flink-0.4.0.0.jar producer.KafkaTrafficIoTSensor localhost:9092
 *   java -classpath streaming-flink-0.4.0.0.jar producer.KafkaTrafficIOTSensor edge2ai-1.dim.local:9092
 *
 *
 * output:
 *   {"sensor_ts":1597076764377,"sensor_id":4,"temp":15,"rain_level":0,"visibility_level":0}
 *
 * @author Marcel Daeppen
 * @version 2020/08/10 11:25
 */

public class KafkaTrafficIOTSensor {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTrafficIOTSensor.class);
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
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-TrafficIOTSensor");
        config.put(ProducerConfig.ACKS_CONFIG,"all");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");
        return new KafkaProducer<>(config);
    }

    private static void publishMessage(Producer<String, byte[]> producer) throws Exception {
        String key = UUID.randomUUID().toString();

        ObjectNode messageJsonObject = jsonObject();
        byte[] valueJson = objectMapper.writeValueAsBytes(messageJsonObject);

        ProducerRecord<String, byte[]> eventrecord = new ProducerRecord<>("TrafficIOTRaw", key, valueJson);

        RecordMetadata md = producer.send(eventrecord).get();
        
        LOG.info(new StringBuilder().append("Published ").append(md.topic()).append("/").append(md.partition()).append("/").append(md.offset()).append(" (key=").append(key).append(") : ").append(messageJsonObject).toString());
    }

    // build random json object
    private static ObjectNode jsonObject() {

        ObjectNode report = objectMapper.createObjectNode();
        report.put("sensor_ts", Instant.now().toEpochMilli());
        report.put("sensor_id", (random.nextInt(11)));
        report.put("temp", (random.nextInt(42 - 20 + 1)));
        report.put("rain_level", (random.nextInt(5)));
        report.put("visibility_level", (random.nextInt(5)));
        return report;
    }

    public static void setsleeptime(long sleeptime) {
        KafkaTrafficIOTSensor.sleeptime = sleeptime;
    }
}