package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * trxStream: {"timestamp":1565604389166,"shop_name":0,"shop_name":"Ums Eck","cc_type":"Revolut","cc_id":"5179-5212-9764-8013","amount_orig":75.86,"fx":"CHF","fx_account":"CHF"}
 *
 * run:
 *    cd /opt/cloudera/parcels/FLINK &&
 *    ./bin/flink run -m yarn-cluster -c consumer.UC5KafkaTrxDuplicateChecker -ynm UC5KafkaTrxDuplicateChecker lib/flink/examples/streaming/streaming-flink-0.1-SNAPSHOT.jar localhost:9092
 *
 *    java -classpath streaming-flink-0.1-SNAPSHOT.jar consumer.UC5KafkaTrxDuplicateChecker
 *
 * @author Marcel Daeppen
 * @version 2020/07/11 12:14
 */

public class UC5KafkaTrxDuplicateChecker {

    private static String brokerURI = "localhost:9092";

    public static void main(String[] args) throws Exception {

        if( args.length == 1 ) {
            System.err.println("case 'customized URI':");
            brokerURI = args[0];
            System.err.println("arg URL: " + brokerURI);
        }else {
            System.err.println("case default");
            System.err.println("default URI: " + brokerURI);
        }

        String use_case_id = "uc5_trx_duplicated_check";
        String topic = "result_" + use_case_id ;

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

        // get trx stream from kafka - topic "cctrx"
        DataStream<String> trxStream = env.addSource(
                new FlinkKafkaConsumer<>("cctrx", new SimpleStringSchema(), properties));

        trxStream.print("input message: ");

        // deserialization of the received JSONObject into Tuple
        DataStream<Tuple2<String, Integer>> aggStream = trxStream
                .flatMap(new trxJSONDeserializer())
                // group by "trx_fingerprint" and sum their occurrences
                .keyBy(0) // trx_fingerprint
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .sum(1)
                 // filter out if the fingerprint is unique within the window {30 sec}. if the fingerprint occurs several times send alarm event
                .filter(new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> value) throws Exception {
                        return value.f1 != 1;
                    }
                });

        aggStream.print(topic + ": ");

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<Tuple2<String, Integer>> myProducer = new FlinkKafkaProducer<Tuple2<String, Integer>>(
                topic, new serializeSum2String(), propertiesProducer);

        aggStream.addSink(myProducer);

        // execute program
        JobExecutionResult result = env.execute(use_case_id);
        JobID jobId = result.getJobID();
        System.err.println("jobId=" + jobId);
    }

    public static class trxJSONDeserializer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private transient ObjectMapper jsonParser;

        /**
         * Select the cc_id, fx, fx_amount, aount_orig from the incoming JSON text as trx_fingerprint.
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            // build trx-fingerprint tuple
            String trx_fingerprint = jsonNode.get("cc_id") + "_" + jsonNode.get("fx") + "_" + jsonNode.get("fx_account") + "_" + jsonNode.get("amount_orig");
            out.collect(new Tuple2<>(trx_fingerprint, 1));
        }
    }

    public static class serializeSum2String implements KeyedSerializationSchema<Tuple2<String, Integer>> {
        @Override
        public byte[] serializeKey(Tuple2 element) {
            return (null);
        }
        @Override
        public byte[] serializeValue(Tuple2 value) {

            String str = "{"+ value.getField(0).toString()
                    + ":" + value.getField(1).toString() + "}";
            return str.getBytes();
        }
        @Override
        public String getTargetTopic(Tuple2 tuple3) {
            // use always the default topic
            return null;
        }
    }

}