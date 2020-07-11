package producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.*;

import static java.util.Collections.unmodifiableList;


/**
 * run:
 *   cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 *   java -classpath flink-simple-tutorial-0.1-SNAPSHOT.jar producer.KafkaOPCSimulator localhost:9092
 *
 * @author Marcel Daeppen
 * @version 2020/07/11 12:14
 */

public class KafkaOPCSimulator {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Random random = new Random();
    private static final List<String> tagname_list = unmodifiableList(Arrays.asList(
            "Triangle1", "Triangle4711", "Sinusoid", "Sawtooth"));
    private static final List<String> unit_list = unmodifiableList(Arrays.asList(
            "opc1", "opc2", "opc_APAC", "Hydrocracker"));

    private static String brokerURI = "localhost:9092";
    private static long sleeptime;

    public static void main(String args[]) throws Exception {

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

        Producer<String, byte[]> producer = createProducer();
        try {
            for (int i = 0; i < 1000000; i++) {
                publishMessage(producer);
                Thread.sleep(sleeptime);
            }
        } finally {
            producer.close();
        }
    }

    private static Producer<String, byte[]> createProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "OPC-Feeder");
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

        ProducerRecord<String, byte[]> record = new ProducerRecord<>("opc", key, valueJson);

        RecordMetadata md = producer.send(record).get();
        System.err.println("Published " + md.topic() + "/" + md.partition() + "/" + md.offset()
                + " (key=" + key + ") : " + messageJsonObject);
    }

    // build random json object
    private static ObjectNode jsonObject() {

        ObjectNode report = objectMapper.createObjectNode();
        report.put("ts", Instant.now().toString());
        report.put("tagname", tagname_list.get(random.nextInt(tagname_list.size())));
        report.put("unit", unit_list.get(random.nextInt(unit_list.size())));
        report.put("val", (random.nextInt(123456)) / 100000.0);
        return report;
    }

    public static void setsleeptime(long sleeptime) {
        KafkaOPCSimulator.sleeptime = sleeptime;
    }
}