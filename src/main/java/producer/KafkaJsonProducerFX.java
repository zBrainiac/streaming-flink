package producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;


/**
 * run:
 *   cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 *   java -classpath streaming-flink-0.2-SNAPSHOT.jar producer.KafkaJsonProducerFX localhost:9092
 *
 * @author Marcel Daeppen
 * @version 2020/07/11 12:14
 */

public class KafkaJsonProducerFX {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Random random = new Random();

    private static String brokerURI = "localhost:9092";
    private static long sleeptime;

    public static void main(String[] args) throws Exception {

        if( args.length == 1 ) {
            System.err.println("case 'customized URI':");
            brokerURI = args[0];
            System.err.println("arg URL: " + brokerURI);
            setsleeptime(1000);
            System.err.println("default sleeptime (ms): " + sleeptime);
        } else if( args.length == 2 ) {
            System.err.println("case 'customized URI & time':");
            brokerURI = args[0];
            setsleeptime(Long.parseLong(args[1]));
            System.err.println("arg URL: " + brokerURI);
            System.err.println("sleeptime (ms): " + sleeptime);
        }else {
            System.err.println("case default");
            System.err.println("default URI: " + brokerURI);
            setsleeptime(1000);
            System.err.println("default sleeptime (ms): " + sleeptime);
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
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-FX");
        config.put(ProducerConfig.ACKS_CONFIG,"1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");
        return new KafkaProducer<>(config);
    }

    private static void publishMessage(Producer<String, byte[]> producer) throws Exception {
        String key = UUID.randomUUID().toString();

        ObjectNode messageJsonObject = jsonObject();
        byte[] valueJson = objectMapper.writeValueAsBytes(messageJsonObject);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>("fxRate", key, valueJson);

        RecordMetadata md = producer.send(record).get();
        System.err.println("Published " + md.topic() + "/" + md.partition() + "/" + md.offset()
                + " (key=" + key + ") : " + messageJsonObject);
    }

    // build random json object
    private static ObjectNode jsonObject() {

        int i= random.nextInt(8);

        ObjectNode report = objectMapper.createObjectNode();
        report.put("timestamp", System.currentTimeMillis());

        String fxRate = "fx_rate";
        switch (i) {
            case 0:
                report.put("fx", "CHF");
                report.put(fxRate, 1.00);
                report.put(fxRate, "CHF");
                break;
            case 1:
                report.put("fx", "CHF");
                report.put(fxRate, "USD");
                report.put(fxRate, (random.nextInt(20) + 90) / 100.0);
                break;
            case 2:
                report.put("fx", "CHF");
                report.put(fxRate, "EUR");
                report.put(fxRate, (random.nextInt(20) + 90) / 100.0);
                break;
            case 3:
                report.put("fx", "EUR");
                report.put(fxRate, 1.00);
                report.put(fxRate, "EUR");
                break;
            case 4:
                report.put("fx", "EUR");
                report.put(fxRate, "USD");
                report.put(fxRate, (random.nextInt(20) + 90) / 100.0);
                break;
            case 5:
                report.put("fx", "EUR");
                report.put(fxRate, "CHF");
                report.put(fxRate, (random.nextInt(20) + 90) / 100.0);
                break;
            case 6:
                report.put("fx", "USD");
                report.put(fxRate, 1.00);
                report.put(fxRate, "USD");
                break;
            case 7:
                report.put("fx", "USD");
                report.put(fxRate, "CHF");
                report.put(fxRate, (random.nextInt(20) + 90) / 100.0);
                break;
            case 8:
                report.put("fx", "USD");
                report.put(fxRate, "EUR");
                report.put(fxRate, (random.nextInt(20) + 90) / 100.0);
                break;
            default:
                System.err.println("i out of range");

        }
        return report;
    }

    public static void setsleeptime(long sleeptime) {
        KafkaJsonProducerFX.sleeptime = sleeptime;
    }
}