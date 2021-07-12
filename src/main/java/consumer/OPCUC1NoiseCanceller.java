package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * opcStream = {"val":"-0.813473","tagname":"Sinusoid","unit":"Hydrocracker","ts":"2020-03-13T11:17:53Z"}
 *
 * run:
 *    cd /opt/cloudera/parcels/FLINK &&
 *    ./bin/flink run -m yarn-cluster -c consumer.OPCUC1NoiseCanceller -ynm OPCUC1NoiseCanceller lib/flink/examples/streaming/streaming-flink-0.4.1.0.jar localhost:9092
 *
 *    java -classpath streaming-flink-0.4.1.0.jar consumer.OPCUC1NoiseCanceller
 *
 * @author Marcel Daeppen
 * @version 2020/07/11 12:14
 */

public class OPCUC1NoiseCanceller {

    private static final Logger LOG = LoggerFactory.getLogger(OPCUC1NoiseCanceller.class);
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

        String use_case_id = "opc_uc1_NoiseCanceller";
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

        DataStream<String> opcStream = env.addSource(
                new FlinkKafkaConsumer<>("opc", new SimpleStringSchema(), properties));

        /* opcStream.print("input message: "); */

        DataStream<Tuple4<String, String, Double, Integer>> aggStream = opcStream
                .flatMap(new TrxJSONDeserializer())
                .keyBy(1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .reduce(new ReduceFunc())
                .filter(new FilterFunction<Tuple4<String, String, Double, Integer>>() {
                    @Override
                    public boolean filter(Tuple4<String, String, Double, Integer> value) throws Exception {

                        if (value.f1.equals("Sinusoid") || value.f1.equals("Sawtooth")) {
                            return true;
                        }
                        return false;
                    }
                });

        aggStream.print(topic + ": ");

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<Tuple4<String, String, Double, Integer>> myProducer = new FlinkKafkaProducer<>(
                topic, new SerializeSum2String(), propertiesProducer);

        aggStream.addSink(myProducer);

        // execute program
        JobExecutionResult result = env.execute(use_case_id);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }


    public static class TrxJSONDeserializer implements FlatMapFunction<String, Tuple4<String, String, Double, Integer>> {
        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String value, Collector<Tuple4<String, String, Double, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            /*
             get sensor_ts, sensor_id, sensor_0 AND sensor_1 from JSONObject
             {"val":"-0.813473","tagname":"Sinusoid","unit":"Hydrocracker","ts":"2020-03-13T11:17:53Z"}
            */

            String ts = jsonNode.get("ts").asText();
            String opc_tag = jsonNode.get("tagname").asText();
            Double opc_value = jsonNode.get("val").asDouble();
            out.collect(new Tuple4<>(ts, opc_tag, opc_value, 1));
        }

    }


    private static class SerializeSum2String implements KeyedSerializationSchema<Tuple4<String, String, Double, Integer>> {
        @Override
        public byte[] serializeKey(Tuple4 element) {
            return (null);
        }

        @Override
        public byte[] serializeValue(Tuple4 value) {

            String str = "{"
                    + "\"type\"" + ":" + "\"aggregated opc stream over 10sec.\""
                    + "," + "\"ts\"" + ":" + "\"" + value.getField(0).toString() + "\""
                    + "," + "\"tagname\"" + ":" + "\"" + value.getField(1).toString() + "\""
                    + "," + "\"value\"" + ":" + "\"" + value.getField(2).toString() + "\"" + "}";
            return str.getBytes();
        }

        @Override
        public String getTargetTopic(Tuple4 tuple4) {
            // use always the default topic
            return null;
        }
    }


    public static class ReduceFunc implements ReduceFunction<Tuple4<String, String, Double, Integer>> {

        public Tuple4<String, String, Double, Integer> reduce(Tuple4<String, String, Double, Integer> current, Tuple4<String, String, Double, Integer> pre_result) throws Exception {
/*
 System.out.println("reducefunc f0: " + current.f0);
 System.out.println("reducefunc f1: " + current.f1);
 System.out.println("reducefunc f2: " + current.f2);
 System.out.println("reducefunc f2 pre_result: " + pre_result.f2);
 System.out.println("reducefunc f3 new: " + current.f3);
 System.out.println("reducefunc f3 pre_result: " + pre_result.f3);
*/
            return new Tuple4<>(current.f0, current.f1, (current.f2 + pre_result.f2) / (current.f3 + pre_result.f3), current.f3 + pre_result.f3);
        }
    }
}