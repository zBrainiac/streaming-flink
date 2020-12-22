package producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.*;

import static java.util.Collections.unmodifiableList;


/**
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath streaming-flink-0.3.0.1.jar producer.KafkaTrafficCollector localhost:9092
 * java -classpath streaming-flink-0.3.0.1.jar producer.KafkaTrafficCollector edge2ai-1.dim.local:9092
 * <p>
 * <p>
 * output:
 * {"sensor_ts":1596956979295,"sensor_id":8,"probability":50,"sensor_x":47,"typ":"LKW","light":false,"license_plate":"DE 483-5849","toll_typ":"10-day"}
 * {"sensor_ts":1596952895018,"sensor_id":10,"probability":52,"sensor_x":14,"typ":"Bike"}
 *
 * @author Marcel Daeppen
 * @version 2020/08/08 12:14
 */

public class KafkaTrafficCollector {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTrafficCollector.class);
    private static final Random random = new SecureRandom();
    private static final String LOGGERMSG = "Program prop set {}";
    private static final List<String> license_plate_country = unmodifiableList(Arrays.asList(
            "AT", "CH", "DE"));

    private static final List<String> toll_typ = unmodifiableList(Arrays.asList(
            "none", "10-day", "2-month", "Annual"));

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
                System.out.print("outerloop: " + i);
                System.out.printf("%n");

                // innerloop
                int innerloopCount = random.nextInt(11);
                int innerloopsleep = random.nextInt(999) + 500;

                System.out.print("innerloopCount: " + innerloopCount);
                System.out.printf("%n");
                System.out.print("innerloopsleep: " + innerloopsleep);
                System.out.printf("%n");

                for (int ii = 0; ii < innerloopCount; ii++) {
                    System.out.print("innerloop: " + ii);
                    System.out.printf("%n");
                    publishMessage(producer);
                    Thread.sleep(innerloopsleep);
                }
                Thread.sleep(sleeptime);
            }
        }
    }

    private static Producer<String, byte[]> createProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-TrafficCounter");
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");
        return new KafkaProducer<>(config);
    }

    private static void publishMessage(Producer<String, byte[]> producer) throws Exception {
        String key = UUID.randomUUID().toString();

        ObjectNode messageJsonObject = jsonObject();
        byte[] valueJson = objectMapper.writeValueAsBytes(messageJsonObject);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>("TrafficCounterRaw", key, valueJson);

        RecordMetadata md = producer.send(record).get();
        System.err.println("Published " + md.topic() + "/" + md.partition() + "/" + md.offset()
                + " (key=" + key + ") : " + messageJsonObject);
    }

    // build random json object
    private static ObjectNode jsonObject() {

        int i = random.nextInt(3);

        ObjectNode report = objectMapper.createObjectNode();
        report.put("sensor_ts", System.currentTimeMillis());
        report.put("sensor_id", (random.nextInt(11)));
        report.put("probability", (random.nextInt(49) + 50));
        report.put("sensor_x", (random.nextInt(99)));

        String key_license_plate = "license_plate" ;
        String key_toll_typ = "toll_typ" ;

        switch (i) {
            case 0:
                report.put("typ", "Bike");

                report.put(key_license_plate, "n/a");
                report.put(key_toll_typ, "n/a");
                break;
            case 1:
                report.put("typ", "LKW");
                report.put("light", (random.nextBoolean()));
                report.put(key_license_plate, license_plate_country.get(random.nextInt(license_plate_country.size())) + " " + (random.nextInt(998 + 1 - 50) + 50) + "-" + (random.nextInt(8999) + 1000));
                report.put(key_toll_typ, toll_typ.get(random.nextInt(toll_typ.size())));

                break;
            case 2:
                report.put("typ", "PKW");
                report.put("light", (random.nextBoolean()));
                report.put(key_license_plate, license_plate_country.get(random.nextInt(license_plate_country.size())) + " " + (random.nextInt(998 + 1 - 50) + 50) + "-" + (random.nextInt(8999) + 1000));
                report.put(key_toll_typ, toll_typ.get(random.nextInt(toll_typ.size())));
                break;
            default:
                System.err.println("i out of range");

        }
        return report;
    }

    public static void setsleeptime(long sleeptime) {
        KafkaTrafficCollector.sleeptime = sleeptime;
    }
}