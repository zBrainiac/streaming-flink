package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Properties;
import java.util.UUID;


/**
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath streaming-flink-0.4.0.0.jar producer.KafkaIOTSimpleCSVProducer localhost:9092
 * <p>
 * output:
 * 1596953344830, 10, 9d02e657-80c9-4857-b18b-26b58f09ae6c, Test Message #25
 *
 * @author Marcel Daeppen
 * @version 2020/08/08 11:14
 */

public class KafkaIOTSimpleCSVProducer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaIOTSimpleCSVProducer.class);
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

        //properties for producer
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-CSV");
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");

        //create producer
        try (Producer<Integer, String> producer = new KafkaProducer<>(config)) {

            //send messages to my-topic
            for (int i = 0; i < 1000000; i++) {
                int randomNum = new SecureRandom().nextInt(11);
                String uuid = UUID.randomUUID().toString();
                Long unixTime = System.currentTimeMillis();

                ProducerRecord<Integer, String> record = new ProducerRecord<>("iot_CSV", i,
                        unixTime
                                + ", " + randomNum
                                + ", " + uuid
                                + ", Test Message #" + i
                );
                producer.send(record);

                System.err.println("Published " + record.topic() + "/" + record.partition()
                        + "/" + " (key=" + record.key() + ") : " + record.value());

                Thread.sleep(sleeptime);
            }

        }
    }

    public static void setsleeptime(long sleeptime) {
        KafkaIOTSimpleCSVProducer.sleeptime = sleeptime;
    }

}