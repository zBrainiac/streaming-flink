package producer;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;


/**
 * run:
 *   cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 *   java -classpath streaming-flink-0.2-SNAPSHOT.jar producer.KafkaIOTSimpleKVProducer localhost:9092
 *
 * output:
 *  unixTime: 1596953939783, sensor_id: 1, id: ba292ff6-e4db-4776-b70e-2b49edfb6726, Test Message: bliblablub #33
 *
 * @author Marcel Daeppen
 * @version 2020/08/08 11:14
 */

public class KafkaIOTSimpleKVProducer {
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
                int randomNum = ThreadLocalRandom.current().nextInt(11);
                String uuid = UUID.randomUUID().toString();

                long unixTime = System.currentTimeMillis();

                ProducerRecord record = new ProducerRecord<>("iot_KV", i,
                        "unixTime: " + unixTime
                                + ", sensor_id: " + randomNum
                                + ", id: " + uuid
                                + ", Test Message: bliblablub #" + i
                );
                producer.send(record);

                System.err.println("Published " + record.topic() + "/" + record.partition() + "/" + " (key=" + record.key() + ") : " + record.value());
                Thread.sleep(sleeptime);
            }
        }
    }

    public static void setsleeptime(long sleeptime) {
        KafkaIOTSimpleKVProducer.sleeptime = sleeptime;
    }

}