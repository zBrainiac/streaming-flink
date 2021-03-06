package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
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
 * iotStream: {"sensor_ts":1588617762605,"sensor_id":7,"sensor_0":59,"sensor_1":32,"sensor_2":84,"sensor_3":23,"sensor_4":56,"sensor_5":30,"sensor_6":46,"sensor_7":90,"sensor_8":64,"sensor_9":33,"sensor_10":49,"sensor_11":91}
 * Aggregation on "sensor_id"
 *
 * run:
 *    cd /opt/cloudera/parcels/FLINK &&
 *    ./bin/flink run -m yarn-cluster -c consumer.IoTUC3CountEventsPerSensorIdSplitter -ynm IoTUC3CountEventsPerSensorIdSplitter lib/flink/examples/streaming/streaming-flink-0.4.0.0.jar localhost:9092
 *
 *    java -classpath streaming-flink-0.4.0.0.jar consumer.IoTUC3CountEventsPerSensorIdSplitter
 *
 * @author Marcel Daeppen
 * @version 2020/07/11 12:14
 */

public class IoTUC3CountEventsPerSensorIdSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(IoTUC3CountEventsPerSensorIdSplitter.class);
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

        String use_case_id = "iot_uc1_Count_EventsPerSensorIdSplitter";
        String topicVendorA = "result_" + use_case_id + "VendorA";
        String topicVendorB = "result_" + use_case_id + "VendorB";
        String topicVendorX = "result_" + use_case_id + "VendorX";

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        DataStream<String> iotStream = env.addSource(
                new FlinkKafkaConsumer<>("iot", new SimpleStringSchema(), properties));

        /* iotStream.print("input message: "); */

        DataStream<Tuple5<Long, Integer, Integer, Integer, Integer>> aggStream = iotStream
                .flatMap(new TrxJSONDeserializer())
                .keyBy(1) // sensor_id
                .sum(4);

        // Split A //
        DataStream<Tuple5<Long, Integer, Integer, Integer, Integer>> splittedStreamA = aggStream
                .filter(new FilterFunction<Tuple5<Long, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple5<Long, Integer, Integer, Integer, Integer> value) throws Exception {
                        return value.f1 == 1 || value.f1 == 3;
                    }
                });
        splittedStreamA.print("splitted StreamA :");

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<Tuple5<Long, Integer, Integer, Integer, Integer>> myProducerA = new FlinkKafkaProducer<>(
                topicVendorA, new SerializeSum2String(), propertiesProducer);

        splittedStreamA.addSink(myProducerA);

        // Split B //
        DataStream<Tuple5<Long, Integer, Integer, Integer, Integer>> splittedStreamB = aggStream
                .filter(new FilterFunction<Tuple5<Long, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple5<Long, Integer, Integer, Integer, Integer> value) throws Exception {
                        return value.f1 == 2 || value.f1 == 4 || value.f1 == 9;
                    }
                });
        splittedStreamB.print("splitted StreamB :");

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<Tuple5<Long, Integer, Integer, Integer, Integer>> myProducerB = new FlinkKafkaProducer<>(
                topicVendorB, new SerializeSum2String(), propertiesProducer);

        splittedStreamB.addSink(myProducerB);

        // Split X //
        DataStream<Tuple5<Long, Integer, Integer, Integer, Integer>> splittedStreamX = aggStream
                .filter(new FilterFunction<Tuple5<Long, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple5<Long, Integer, Integer, Integer, Integer> value) throws Exception {
                        return value.f1 == 5 || value.f1 == 7 || value.f1 == 6 || value.f1 == 8;
                    }
                });
        splittedStreamX.print("splitted StreamX :");

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<Tuple5<Long, Integer, Integer, Integer, Integer>> myProducerX = new FlinkKafkaProducer<>(
                topicVendorX, new SerializeSum2String(), propertiesProducer);

        splittedStreamX.addSink(myProducerX);

        // execute program
        JobExecutionResult result = env.execute(use_case_id);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }


    public static class TrxJSONDeserializer implements FlatMapFunction<String, Tuple5<Long, Integer, Integer, Integer, Integer>> {
        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String value, Collector<Tuple5<Long, Integer, Integer, Integer, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            // get sensor_ts, sensor_id, sensor_0 AND sensor_1 from JSONObject
            Long sensor_ts = jsonNode.get("sensor_ts").asLong();
            Integer sensor_id = jsonNode.get("sensor_id").asInt();
            Integer sensor_0 = jsonNode.get("sensor_0").asInt();
            Integer sensor_1 = jsonNode.get("sensor_1").asInt();
            out.collect(new Tuple5<>(sensor_ts, sensor_id, sensor_0, sensor_1, 1));

        }

    }

    private static class SerializeSum2String implements KeyedSerializationSchema<Tuple5<Long, Integer, Integer, Integer, Integer>> {
        @Override
        public byte[] serializeKey(Tuple5 element) {
            return (null);
        }

        @Override
        public byte[] serializeValue(Tuple5 value) {

            String str = "{"
                    + "\"type\"" + ":" + "\"alerts for vendor\""
                    + "," + "\"sensor_ts_start\"" + ":" + value.getField(0).toString()
                    + "," + "\"sensor_id\"" + ":" + value.getField(1).toString()
                    + "," + "\"sensor_0\"" + ":" + value.getField(2).toString() + "}";
            return str.getBytes();
        }

        @Override
        public String getTargetTopic(Tuple5 tuple5) {
            // use always the default topic
            return null;
        }
    }

}