package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * csvStream: unixTime :1596022830196, sensor_id :6, id :e78b8a17-527b-4e51-9fdb-577da3207db0, Test Message #198
 * Aggregation on "sensor_id"
 *
 * run:
 *    cd /opt/cloudera/parcels/FLINK &&
 *    ./bin/flink run -m yarn-cluster -c consumer.IoTConsumerCSV -ynm IoTConsumerCSV lib/flink/examples/streaming/streaming-flink-0.3.1.0.jar localhost:9092
 *
 *    java -classpath streaming-flink-0.3.1.0.jar consumer.IoTConsumerCSV
 *
 * @author Marcel Daeppen
 * @version 2020/07/29 14:14
 */

public class IoTUC8ConsumerCSVCheckpointing1000 {

    private static final Logger LOG = LoggerFactory.getLogger(IoTUC8ConsumerCSVCheckpointing1000.class);
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

        String use_case_id = "iot_uc8_Consumer_CSV_Checkpoint1000";
        String topic = "result_" + use_case_id;

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, use_case_id);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, use_case_id);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringConsumerInterceptor");

        Properties propertiesProducer = new Properties();
        propertiesProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        propertiesProducer.put(ProducerConfig.CLIENT_ID_CONFIG, use_case_id);
        propertiesProducer.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");

        DataStream<String> csvStream = env.addSource(
                new FlinkKafkaConsumer<>("iot_CSV", new SimpleStringSchema(), properties));

        DataStream<Tuple5<String, String, String, String, Integer>> aggStream = csvStream
                .map(new MapFunction<String, Tuple5<String, String, String, String, Integer>>() {
                    @Override
                    public Tuple5<String, String, String, String, Integer> map(String str) throws Exception {
                        String[] temp = str.split(",");
                        return new Tuple5<>(
                                String.valueOf(temp[0]).replace("unixTime: ", "") ,
                                String.valueOf(temp[1]).replace("sensor_id: ", ""),
                                String.valueOf(temp[2]).replace("id: ", ""),
                                String.valueOf(temp[3]),
                                1
                        );
                    }})
                .keyBy(1)
                .sum(4);

        aggStream.print(topic + ": ");

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<Tuple5<String, String, String, String, Integer>> myProducer = new FlinkKafkaProducer<>(
                topic, new SerializeSum2String(), propertiesProducer);

        aggStream.addSink(myProducer);

        // execute program
        JobExecutionResult result = env.execute(use_case_id);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }

    private static class SerializeSum2String implements KeyedSerializationSchema<Tuple5<String, String, String, String, Integer>> {
        @Override
        public byte[] serializeKey(Tuple5 element) {
            return (null);
        }

        @Override
        public byte[] serializeValue(Tuple5 value) {

            String str = "{"
                    + "\"type\"" + ":" + "\"counter by sensor_id\""
                    + "," + "\"sensor_ts_start\"" + ":" + value.getField(0).toString()
                    + "," + "\"sensor_id\"" + ":" + value.getField(1).toString()
                    + "," + "\"counter\"" + ":" + value.getField(4).toString() + "}";
            return str.getBytes();
        }

        @Override
        public String getTargetTopic(Tuple5 tuple5) {
            // use always the default topic
            return null;
        }
    }

}