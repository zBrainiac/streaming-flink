package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * iotStream: {"sensor_ts":1588617762605,"sensor_id":7,"sensor_0":59,"sensor_1":32,"sensor_2":84,"sensor_3":23,"sensor_4":56,"sensor_5":30,"sensor_6":46,"sensor_7":90,"sensor_8":64,"sensor_9":33,"sensor_10":49,"sensor_11":91}
 * Aggregation on "sensor_id"
 * <p>
 * run:
 * cd /opt/cloudera/parcels/FLINK &&
 * ./bin/flink run -m yarn-cluster -c consumer.IoTUC10CountEventsPerSensorIdSaveTofile -ynm IoTUC10CountEventsPerSensorIdSaveTofile lib/flink/examples/streaming/streaming-flink-0.4.0.0.jar localhost:9092
 * <p>
 * java -classpath streaming-flink-0.4.0.0.jar consumer.IoTUC10CountEventsPerSensorIdSaveTofile
 *
 * @author Marcel Daeppen
 * @version 2020/07/11 12:14
 */

public class IoTUC10CountEventsPerSensorIdSaveTofile {

    private static final Logger LOG = LoggerFactory.getLogger(IoTUC10CountEventsPerSensorIdSaveTofile.class);
    private static final String LOGGERMSG = "Program prop set {}";
    private static String brokerURI = "localhost:9092";
    private static String outputFolder = "data/output";

    public static void main(String[] args) throws Exception {

        if (args.length == 1) {
            brokerURI = args[0];
            String parm = "'use program argument parm: URI' = " + brokerURI;
            LOG.info(LOGGERMSG, parm);
        } else if (args.length == 2) {
            brokerURI = args[0];
            outputFolder = args[1];
            String parm = "'use customized URI' = " + brokerURI + " & 'use customized output file location' = " + outputFolder;
            LOG.info(LOGGERMSG, parm);
        } else {
            String parm = "'use default URI' = " + brokerURI + " & 'use default output file location' = " + outputFolder;
            LOG.info(LOGGERMSG, parm);
        }

        String use_case_id = "iot_uc10_Count_EventsPerSensorIdSaveToFile";
        String topic = "result_" + use_case_id;

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000); // checkpoint every 30 secs
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

        aggStream.print(topic + ": ");

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<Tuple5<Long, Integer, Integer, Integer, Integer>> myProducer = new FlinkKafkaProducer<>(
                topic, new SerializeSum2String(), propertiesProducer);

        aggStream.addSink(myProducer);

        // sink

        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".ext")
                .build();

        StreamingFileSink<Tuple5<Long, Integer, Integer, Integer, Integer>> sinkfile = StreamingFileSink
                .forRowFormat(new Path(outputFolder), new SimpleStringEncoder<Tuple5<Long, Integer, Integer, Integer, Integer>>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024)  /* 1024 * 1024 */
                                .build())
                .withOutputFileConfig(config)
                .build();

        aggStream.addSink(sinkfile);


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
                    + "\"type\"" + ":" + "\"alert sensor_0 over 50\""
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