package consumer;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;


/**
 * input stream:
 *   {"sensor_ts":1596956979295,"sensor_id":8,"probability":50,"sensor_x":47,"typ":"LKW","light":false,"license_plate":"DE 483-5849","toll_typ":"10-day"}
 *   {"sensor_ts":1596952895018,"sensor_id":10,"probability":52,"sensor_x":14,"typ":"Bike"}
 *
 * Aggregation on "sensor_id" & "typ"
 *
 * run:
 * cd /opt/cloudera/parcels/FLINK &&
 * ./bin/flink run -m yarn-cluster -c consumer.TrafficUC3SummeryPerSensor -ynm TrafficUC3SummeryPerSensor lib/flink/examples/streaming/streaming-flink-0.3.0.1.jar localhost:9092
 * ./bin/flink run -m yarn-cluster -c consumer.TrafficUC3SummeryPerSensor -ynm TrafficUC3SummeryPerSensor lib/flink/examples/streaming/streaming-flink-0.3.0.1.jar edge2ai-1.dim.local:9092
 *
 * java -classpath streaming-flink-0.3.0.1.jar consumer.TrafficUC3SummeryPerSensor
 *
 * @author Marcel Daeppen
 * @version 2020/08/08 12:14
 */

public class TrafficUC3SummeryPerSensor {

    private static String brokerURI = "localhost:9092";

    public static void main(String[] args) throws Exception {

        if (args.length == 1) {
            System.err.println("case 'customized URI':");
            brokerURI = args[0];
            System.err.println("arg URL: " + brokerURI);
        } else {
            System.err.println("case default");
            System.err.println("default URI: " + brokerURI);
        }

        String use_case_id = "Traffic_UC3_SummeryPerSensor";
        String topic = "result_" + use_case_id;

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
                new FlinkKafkaConsumer<>("TrafficCounterRaw", new SimpleStringSchema(), properties));

        iotStream.print("input message: ");

        DataStream<Tuple8<Long, Integer, String, Integer, String, Integer, String, Integer>> aggStream = iotStream
                .flatMap(new TrxJSONDeserializer())
                .keyBy(1) // sensor_id & typ
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .sum(7)
                ;

        aggStream.print(topic + ": ");

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<Tuple8<Long, Integer, String, Integer, String, Integer, String, Integer>> myProducer = new FlinkKafkaProducer<>(
                topic, new SerializeSum2String(), propertiesProducer);

        aggStream.addSink(myProducer);

        // execute program
        JobExecutionResult result = env.execute(use_case_id);
        JobID jobId = result.getJobID();
        System.err.println("jobId=" + jobId);
    }


    public static class TrxJSONDeserializer implements FlatMapFunction<String, Tuple8<Long, Integer, String, Integer, String, Integer, String, Integer>> {
        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String value, Collector<Tuple8<Long, Integer, String, Integer, String, Integer, String, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            // get sensor_ts, sensor_id, typ AND sensor_1 from JSONObject
            Long sensor_ts = jsonNode.get("sensor_ts").asLong();
            Integer sensor_id = jsonNode.get("sensor_id").asInt();
            String typ = jsonNode.get("typ").asText();
            Integer probability = jsonNode.get("probability").asInt();
            Integer sensor_x = jsonNode.get("sensor_x").asInt();
            String toll_typ = jsonNode.get("toll_typ").asText();
            String license_plate = jsonNode.get("license_plate").asText();
            out.collect(new Tuple8<>(sensor_ts, sensor_id, typ, probability, license_plate,sensor_x,toll_typ,1));
        }
    }

    private static class SerializeSum2String implements KeyedSerializationSchema<Tuple8<Long, Integer, String, Integer, String, Integer, String, Integer>> {
        @Override
        public byte[] serializeKey(Tuple8 element) {
            return (null);
        }

        @Override
        public byte[] serializeValue(Tuple8 value) {

            String str = "{"
                    + "\"type\"" + ":" + "\"Info\""
                    + "," + "\"subtype\"" + ":" + "\"Counter by sensor_id and typ over 30 sec.\""
                    + "," + "\"sensor_ts_start\"" + ":" + value.getField(0).toString()
                    + "," + "\"sensor_id\"" + ":" + value.getField(1).toString()
                    + "," + "\"counter\"" + ":" + value.getField(7).toString() + "}";
            return str.getBytes();
        }

        @Override
        public String getTargetTopic(Tuple8 tuple5) {
            // use always the default topic
            return null;
        }
    }

}