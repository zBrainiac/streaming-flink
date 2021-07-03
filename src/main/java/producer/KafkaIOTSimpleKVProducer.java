package producer;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Properties;
import java.util.UUID;


/**
 * run:
 *   cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 *   java -classpath streaming-flink-0.4.0.0.jar producer.KafkaIOTSimpleKVProducer localhost:9092
 *
 * output:
 *  unixTime: 1596953939783, sensor_id: 1, id: ba292ff6-e4db-4776-b70e-2b49edfb6726, Test Message: bliblablub #33
 *
 * @author Marcel Daeppen
 * @version 2020/08/08 11:14
 */

public class KafkaIOTSimpleKVProducer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaIOTSimpleKVProducer.class);
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
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-KV");
        config.put(ProducerConfig.ACKS_CONFIG,"1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");

        //create producer
        try (Producer<Integer, String> producer = new KafkaProducer<>(config)) {

            //send messages to my-topic
            for (int i = 0; i < 1000000; i++) {
                int randomNum = new SecureRandom().nextInt(11);
                String uuid = UUID.randomUUID().toString();

                long unixTime = System.currentTimeMillis();

                ProducerRecord<Integer, String> eventrecord = new ProducerRecord<>("iot_KV", i,
                        "unixTime: " + unixTime
                                + ", sensor_id: " + randomNum
                                + ", id: " + uuid
                                + ", Test Message: bliblablub #" + i
                );
                producer.send(eventrecord);

                LOG.info(new StringBuilder().append("Published ").append(eventrecord.topic()).append("/").append(eventrecord.partition()).append("/").append(" (key=").append(eventrecord.key()).append(") : ").append(eventrecord.value()).toString());
                Thread.sleep(sleeptime);
            }
        }
    }

    public static void setsleeptime(long sleeptime) {
        KafkaIOTSimpleKVProducer.sleeptime = sleeptime;
    }

}