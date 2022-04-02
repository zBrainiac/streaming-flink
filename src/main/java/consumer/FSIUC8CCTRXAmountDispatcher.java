package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * trxStream: {"timestamp":1565604389166,"shop_name":0,"shop_name":"Ums Eck","cc_type":"Revolut","cc_id":"5179-5212-9764-8013","amount_orig":75.86,"fx":"CHF","fx_account":"CHF"}
 * Aggregation on "shop_name" & "fx"
 *
 * run:
 *    cd /opt/cloudera/parcels/FLINK &&
 *    ./bin/flink run -m yarn-cluster -c consumer.FSIUC8KafkaTRXAmountDispatcher -ynm FSIUC8KafkaTRXAmountDispatcher lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092
 *
 *    java -classpath streaming-flink-0.5.0.0.jar consumer.FSIUC8KafkaTRXAmountDispatcher
 *
 * @author Marcel Daeppen
 * @version 2022/02/06 12:14
 */

public class FSIUC8CCTRXAmountDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(FSIUC8CCTRXAmountDispatcher.class);
    private static String brokerURI = "localhost:9092";
    private static final String LOGGMSG = "Program prop set {}";

    public static void main(String[] args) throws Exception {

        if( args.length == 1 ) {
            brokerURI = args[0];
            String parm = "'use program argument parm: URI' = " + brokerURI;
            LOG.info(LOGGMSG, parm);
        }else {
            String parm = "'use default URI' = " + brokerURI;
            LOG.info(LOGGMSG, parm);
        }

        String usecaseid = "FSIUC8CCTRXAmountDispatcher";
        String topicAbove40 = "result_" + usecaseid + "Above40Stream";
        String topicBelow40 = "result_" + usecaseid + "Below40Stream";

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, usecaseid);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, usecaseid);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringConsumerInterceptor");

        Properties propertiesProducer = new Properties();
        propertiesProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        propertiesProducer.put(ProducerConfig.CLIENT_ID_CONFIG, usecaseid);
        propertiesProducer.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");

        KafkaSource<String> eventStream = KafkaSource.<String>builder()
                .setBootstrapServers(brokerURI)
                .setTopics("cctrx")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        DataStream<Tuple10<Integer, String, String, Double, Integer, String, String, String, String, Integer>> Above40Stream = env.fromSource(
                        eventStream,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source")
                .flatMap(new FSIJSONDeserializerCreditCardTrxTuple10())
                .filter(value -> value.f3 >= 40.01);

        Above40Stream.print("Above40Stream :");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokerURI)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicAbove40)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(propertiesProducer)
                .build();

        Above40Stream
                .map((MapFunction<Tuple10<Integer, String, String, Double, Integer, String, String, String, String, Integer>, String>) s -> "{"
                        + "\"type\"" + ":" + "\"" + topicAbove40 + "\""
                        + "," + "\"cc_ic\"" + ":" + "\"" + s.f1 + "\""
                        + "," + "\"cc_type\"" + ":" + "\"" + s.f2 + "\""
                        + "," + "\"amount_orig\"" + ":" + s.f3
                        + "," + "\"fx\"" + ":"  + s.f6 + "}")
                .sinkTo(kafkaSink).name("Equipment Kafka Destination");


        DataStream<Tuple10<Integer, String, String, Double, Integer, String, String, String, String, Integer>> Below40Stream = env.fromSource(
                        eventStream,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source")
                .flatMap(new FSIJSONDeserializerCreditCardTrxTuple10())
                .filter(value -> value.f3 <= 40.01);

        Below40Stream.print("Below40Stream :");

        KafkaSink<String> kafkaSinkBelow40Stream = KafkaSink.<String>builder()
                .setBootstrapServers(brokerURI)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicBelow40)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(propertiesProducer)
                .build();

        Below40Stream
                .map((MapFunction<Tuple10<Integer, String, String, Double, Integer, String, String, String, String, Integer>, String>) s -> "{"
                        + "\"type\"" + ":" + "\"" + topicBelow40 + "\""
                        + "," + "\"cc_ic\"" + ":" + "\"" + s.f1 + "\""
                        + "," + "\"cc_type\"" + ":" + "\"" + s.f2 + "\""
                        + "," + "\"amount_orig\"" + ":" + s.f3
                        + "," + "\"fx\"" + ":"  + s.f6 + "}")
                .sinkTo(kafkaSinkBelow40Stream).name("Equipment Kafka Destination");






        // execute program
        JobExecutionResult result = env.execute(usecaseid);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }

}