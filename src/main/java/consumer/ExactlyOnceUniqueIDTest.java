package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
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
 *    ./bin/flink run -m yarn-cluster -c consumer.ExactlyOnceUniqueIDTest -ynm ExactlyOnceUniqueIDTest lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar localhost:9092
 *
 *    java -classpath streaming-flink-0.4.1.0.jar consumer.ExactlyOnceUniqueIDTest
 *
 * @author Marcel Daeppen
 * @version 2021/12/08 10:12
 */

public class ExactlyOnceUniqueIDTest {

    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceUniqueIDTest.class);
    private static String brokerURI = "localhost:9092";
    private static final String LOGGERMSG = "Program prop set {}";

    public static void main(String[] args) throws Exception {

        if( args.length == 1 ) {
            brokerURI = args[0];
            String parm = "'use program argument parm: URI' = " + brokerURI;
            LOG.info(LOGGERMSG, parm);
        }else {
            String parm = "'use default URI' = " + brokerURI;
            LOG.info(LOGGERMSG, parm);
        }

        String use_case_id = "ExactlyOnceUniqueIDTest";
        String topic = "result_" + use_case_id;

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, use_case_id);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, use_case_id);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        Properties propertiesProducer = new Properties();
        propertiesProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        propertiesProducer.put(ProducerConfig.CLIENT_ID_CONFIG, use_case_id);
        propertiesProducer.put(ProducerConfig.ACKS_CONFIG, "all");
        propertiesProducer.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "ExactlyOnceUniqueIDTest");
        propertiesProducer.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        propertiesProducer.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"1000");


        DataStream<String> csvStream = env.addSource(
                new FlinkKafkaConsumer<>("kafka_simple_transactional", new SimpleStringSchema(), properties));

        DataStream<Tuple4<Integer, String, Integer, Integer>> aggStream = csvStream
/*                .map(new MapFunction<String, Tuple4<Integer, String, Integer, Integer>>() {
                    @Override
                    public Tuple4<Integer, String, Integer, Integer> map(String str) throws Exception {
                        String[] temp = str.split(",");
                        return new Tuple4<>(
                                String.valueOf(temp[0]).replace("msg_id:", ""),
                                String.valueOf(temp[1]).replace("Current_time_is:", ""),
                                //      String.format("%012d", getNextCode()),
                                (int) getNextCode(),
                                1
                        );
                }})*/
                .keyBy(0)
                .flatMap(new FlatMapFunction<String, Tuple4<Integer, String, Integer, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple4<Integer, String, Integer, Integer>> collector) throws Exception {

                    }
                });
//                .flatMap(new StatefulMap())
                  ;


        LOG.info("marcel");
        aggStream.print(topic + ": ");

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<Tuple4<Integer, String, Integer, Integer>> myProducer = new FlinkKafkaProducer<>(
                topic,
                new SerializeSum2String(),
                propertiesProducer);

        aggStream.addSink(myProducer);


        // execute program
        JobExecutionResult result = env.execute(use_case_id);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }

    private static class SerializeSum2String implements KeyedSerializationSchema<Tuple4<Integer, String, Integer, Integer>> {
        @Override
        public byte[] serializeKey(Tuple4 element) {
            return (null);
        }

        @Override
        public byte[] serializeValue(Tuple4 value) {

            String str = "{"
                    + "\"type\"" + ":" + "\"TEST: Exactly Once\""
                    + "," + "\"msg_id\"" + ":" + value.getField(0).toString()
                    + "," + "\"unique_seq_id\"" + ":" + value.getField(2)
                    + "," + "\"Current_time_is\"" + ":" + value.getField(1).toString() + "}";
            return str.getBytes();
        }

        @Override
        public String getTargetTopic(Tuple4 tuple5) {
            // use always the default topic
            return null;
        }
    }

}

