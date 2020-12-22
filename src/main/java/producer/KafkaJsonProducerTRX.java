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
 * java -classpath streaming-flink-0.3.0.1.jar producer.KafkaJsonProducerTRX localhost:9092
 *
 * @author Marcel Daeppen
 * @version 2020/07/11 12:14
 */

public class KafkaJsonProducerTRX {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonProducerTRX.class);
    private static final Random random = new SecureRandom();
    private static final String LOGGERMSG = "Program prop set {}";
    private static final List<String> transaction_card_type_list = unmodifiableList(Arrays.asList(
            "Visa", "MasterCard", "Maestro", "AMEX", "Diners Club", "Revolut"));
    private static final List<String> transaction_currency_list = unmodifiableList(Arrays.asList(
            "USD", "EUR", "CHF"));

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
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-CC-TRX");
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

        ProducerRecord<String, byte[]> record = new ProducerRecord<>("cctrx", key, valueJson);

        RecordMetadata md = producer.send(record).get();
        System.err.println("Published " + md.topic() + "/" + md.partition() + "/" + md.offset()
                + " (key=" + key + ") : " + messageJsonObject);
    }

    // build random json object
    private static ObjectNode jsonObject() {

        int i = random.nextInt(5);

        ObjectNode report = objectMapper.createObjectNode();
        report.put("timestamp", System.currentTimeMillis());
        report.put("cc_id", "51" + (random.nextInt(89) + 10) + "-" + (random.nextInt(8999) + 1000) + "-" + (random.nextInt(8999) + 1000) + "-" + (random.nextInt(8999) + 1000));
        report.put("cc_type", transaction_card_type_list.get(random.nextInt(transaction_card_type_list.size())));
        report.put("shop_id", i);

        String shopName = "shop_name";
        switch (i) {
            case 0:
                report.put(shopName, "Tante_Emma");
                break;
            case 1:
                report.put(shopName, "Aus_der_Region");
                break;
            case 2:
                report.put(shopName, "Shop_am_Eck");
                break;
            case 3:
                report.put(shopName, "SihlCity");
                break;
            case 4:
                report.put(shopName, "BioMarkt");
                break;
            default:
                System.err.println("i out of range");
        }

        report.put("fx", transaction_currency_list.get(random.nextInt(transaction_currency_list.size())));
        report.put("fx_account", transaction_currency_list.get(random.nextInt(transaction_currency_list.size())));
        report.put("amount_orig", (random.nextInt(8900) + 10) / 100.0);
        return report;
    }

    public static void setsleeptime(long sleeptime) {
        producer.KafkaJsonProducerTRX.sleeptime = sleeptime;
    }
}