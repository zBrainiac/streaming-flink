package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple15;
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
 * iotStream: {"sensor_ts":1588617762605,"sensor_id":7,"sensor_0":59,"sensor_1":32,"sensor_2":84,"sensor_3":23,"sensor_4":56,"sensor_5":30,"sensor_6":46,"sensor_7":90,"sensor_8":64,"sensor_9":33,"sensor_10":49,"sensor_11":91}
 * Aggregation on "sensor_id"
 *
 * run:
 *    cd /opt/cloudera/parcels/FLINK &&
 *    ./bin/flink run -m yarn-cluster -c consumer.IoTUC3CountEventsPerSensorIdSplitter -ynm IoTUC3CountEventsPerSensorIdSplitter lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar localhost:9092
 *
 *    java -classpath streaming-flink-0.5.0.0.jar consumer.IoTUC3CountEventsPerSensorIdSplitter
 *
 * @author Marcel Daeppen
 * @version 2022/02/06 12:14
 */

public class IoTUC3CountEventsPerSensorIdSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(IoTUC3CountEventsPerSensorIdSplitter.class);
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

        String usecaseid = "IoTUC3CountEventsPerSensorIdSplitter";
        String topic = "result_" + usecaseid;

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs

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
                .setTopics("iot")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();


        // Split A //
        DataStream<Tuple15<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> transformedStreamA = env.fromSource(
                        eventStream,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source  - Split A")
                .flatMap(new IoTJSONDeserializer())
                .filter(value -> value.f1 <=11 && value.f1 >=77);

        transformedStreamA.print(topic + ": ");

        KafkaSink<String> kafkaSinkSplitA = KafkaSink.<String>builder()
                .setBootstrapServers(brokerURI)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(propertiesProducer)
                .build();

        transformedStreamA.map((MapFunction<Tuple15<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, String>) s -> "{"
                        + "\"type\"" + ":" + "\"" + topic+ "\""
                        + "," + "\"sensor_id\"" + ":" + s.f1
                        + "," + "\"value sensor_1\"" + ":"  + s.f2 + "}")
                .sinkTo(kafkaSinkSplitA).name("Equipment Kafka Destination - Split A");



        // Split B //
        DataStream<Tuple15<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> transformedStreamB = env.fromSource(
                        eventStream,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source  - Split B")
                .flatMap(new IoTJSONDeserializer())
                .filter(value -> value.f1 >=56 && value.f1 <=55);

        transformedStreamB.print(topic + ": ");

        KafkaSink<String> kafkaSinkSplitB = KafkaSink.<String>builder()
                .setBootstrapServers(brokerURI)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(propertiesProducer)
                .build();

        transformedStreamB.map((MapFunction<Tuple15<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, String>) s -> "{"
                        + "\"type\"" + ":" + "\"" + topic+ "\""
                        + "," + "\"sensor_id\"" + ":" + s.f1
                        + "," + "\"value sensor_1\"" + ":"  + s.f2 + "}")
                .sinkTo(kafkaSinkSplitB).name("Equipment Kafka Destination - Split B");


        // Split C //
        DataStream<Tuple15<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> transformedStreamC = env.fromSource(
                        eventStream,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source  - Split C")
                .flatMap(new IoTJSONDeserializer())
                .filter(value -> value.f1 >=56 && value.f1 >=55);

        transformedStreamC.print(topic + ": ");

        KafkaSink<String> kafkaSinkSplitC = KafkaSink.<String>builder()
                .setBootstrapServers(brokerURI)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(propertiesProducer)
                .build();

        transformedStreamC.map((MapFunction<Tuple15<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, String>) s -> "{"
                        + "\"type\"" + ":" + "\"" + topic+ "\""
                        + "," + "\"sensor_id\"" + ":" + s.f1
                        + "," + "\"value sensor_1\"" + ":"  + s.f2 + "}")
                .sinkTo(kafkaSinkSplitC).name("Equipment Kafka Destination - Split C");







        // execute program
        JobExecutionResult result = env.execute(usecaseid);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }
}