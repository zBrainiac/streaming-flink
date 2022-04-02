
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
 *
 * run:
 *    cd /opt/cloudera/parcels/FLINK &&
 *    ./bin/flink run -m yarn-cluster -c consumer.FSIUC2SumccTypTrxFx -ynm FSIUC2SumccTypTrxFx lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar localhost:9092
 *
 *    java -classpath streaming-flink-0.5.0.0.jar consumer.FSIUC2SumccTypTrxFx
 *
 * @author Marcel Daeppen
 * @version 2022/02/05 18:12
 */

class FSIUC2SumccTypTrxFx {

    private static final Logger LOG = LoggerFactory.getLogger(FSIUC2SumccTypTrxFx.class);
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

        String usecaseId = "FSIUC2SumccTypTrxFx";
        String topic = "result_" + usecaseId;

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, usecaseId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, usecaseId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        Properties propertiesProducer = new Properties();
        propertiesProducer.put(ProducerConfig.CLIENT_ID_CONFIG, usecaseId);
        propertiesProducer.put(ProducerConfig.ACKS_CONFIG, "all");
        propertiesProducer.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);


        KafkaSource<String> eventStream = KafkaSource.<String>builder()
                .setBootstrapServers(brokerURI)
                .setTopics("cctrx")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        DataStream<Tuple10<Integer, String, String, Double, Integer, String, String, String, String, Integer>> transformedStream = env.fromSource(
                        eventStream,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source")
                .flatMap(new FSIJSONDeserializerCreditCardTrxTuple10())
                .keyBy(2,6)
                .sum(3);

        transformedStream.print(topic + ": ");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokerURI)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(propertiesProducer)
                .build();

        transformedStream.map((MapFunction<Tuple10<Integer, String, String, Double, Integer, String, String, String, String, Integer>, String>) s -> "{"
                        + "\"type\"" + ":" + "\"" + topic+ "\""
                        + "," + "\"cctype\"" + ":" + s.f2
                        + "," + "\"fx\"" + ":" + s.f6
                        + "," + "\"sum orig amount\"" + ":" + "\"" + s.f3 + "\"" + "}")
                .sinkTo(kafkaSink).name("Equipment Kafka Destination");

        // execute program
        JobExecutionResult result = env.execute(usecaseId);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }
}