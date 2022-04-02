package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * csvStream: msg_id:295, Current_time_is:2021-12-08T09:05:00.440Z
 *
 * run:
 *    cd /opt/cloudera/parcels/FLINK &&
 *    ./bin/flink run -m yarn-cluster -c consumer.ExactlyOnce -ynm ExactlyOnce lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar localhost:9092
 *
 *    java -classpath streaming-flink-0.5.0.0.jar consumer.ExactlyOnce
 *
 * @author Marcel Daeppen
 * @version 2021/12/08 10:12
 */

public class ExactlyOnce {

    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnce.class);
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

        String usecaseId = "ExactlyOnce";
        String topic = "result_" + usecaseId;

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, usecaseId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, usecaseId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        Properties propertiesProducer = new Properties();
        propertiesProducer.put(ProducerConfig.CLIENT_ID_CONFIG, usecaseId);
        propertiesProducer.put(ProducerConfig.ACKS_CONFIG, "all");
        propertiesProducer.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        propertiesProducer.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"1000");


        KafkaSource<String> eventStream = KafkaSource.<String>builder()
                .setBootstrapServers(brokerURI)
                .setTopics("kafka_simple_transactional")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        DataStream<Tuple3<String, String, Integer>> transformedStream = env.fromSource(
                        eventStream,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source")
                .map(new MapFunction<String, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> map(String str) {
                        String[] temp = str.split(",");
                        return new Tuple3<>(
                                String.valueOf(temp[0]).replace("msg_id:", ""),
                                String.valueOf(temp[1]).replace("Current_time_is:", ""),
                                1
                        );
                    }});

        transformedStream.print(topic + ": ");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokerURI)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(propertiesProducer)
                .build();


        transformedStream.map((MapFunction<Tuple3<String, String, Integer>, String>) s -> "{"
                        + "\"type\"" + ":" + "\"TEST - Exactly Once\""
                        + "," + "\"msg_id\"" + ":" + s.f0
                        + "," + "\"Current_time_is\"" + ":"+"\"" + s.f1 + "\"" + "}")
                .sinkTo(kafkaSink).name("Equipment Kafka Destination");

        // execute program
        JobExecutionResult result = env.execute(usecaseId);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }
}