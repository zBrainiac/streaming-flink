package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
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
 * trxStream: {"timestamp":1565604389166,"shop_name":0,"shop_name":"Ums Eck","cc_type":"Revolut","cc_id":"5179-5212-9764-8013","amount_orig":75.86,"fx":"CHF","fx_account":"CHF"}
 * Aggregation on "shop_name" & "fx"
 *
 * run:
 *    cd /opt/cloudera/parcels/FLINK &&
 *    ./bin/flink run -m yarn-cluster -c consumer.FSIUC2KafkaSumccTypTrxFx -ynm FSIUC2KafkaSumccTypTrxFx lib/flink/examples/streaming/streaming-flink-0.3.1.0.jar localhost:9092
 *
 *    java -classpath streaming-flink-0.3.1.0.jar consumer.FSIUC2KafkaSumccTypTrxFx
 *
 * @author Marcel Daeppen
 * @version 2020/07/11 12:14
 */

public class FSIUC2KafkaSumccTypTrxFx {

    private static final Logger LOG = LoggerFactory.getLogger(FSIUC2KafkaSumccTypTrxFx.class);
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

        String use_case_id = "fsi-uc2_trx_typ_fx";
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

        /* trxStream.print("input message: "); */

        // deserialization of the received JSONObject into Tuple
        DataStream<Tuple5<String, String, String, String, Double>> aggStream = trxStream
                .flatMap(new TrxJSONDeserializer())
                // group by "cc_typ" AND "fx" and sum their occurrences
                .keyBy(0,1)
                .sum(4 );

        aggStream.print(topic + ": ");

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<Tuple5<String, String, String, String, Double>> myProducer = new FlinkKafkaProducer<>(
                topic, new SerializeTuple5toString(), propertiesProducer);

        aggStream.addSink(myProducer);

        // execute program
        JobExecutionResult result = env.execute(use_case_id);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }

    public static class TrxJSONDeserializer implements FlatMapFunction<String, Tuple5<String, String, String, String, Double>> {
        private transient ObjectMapper jsonParser;

        /**
         * Select the shop name from the incoming JSON text.
         */
        @Override
        public void flatMap(String value, Collector<Tuple5<String, String, String, String, Double>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            // get shop_name AND fx from JSONObject
            String cc_type = jsonNode.get("cc_type").toString();
            Double amount_orig = jsonNode.get("amount_orig").asDouble();
            String fx = jsonNode.get("fx").toString();
            String fx_account = jsonNode.get("fx_account").toString();
            String fx_fx = jsonNode.get("fx") + "_" + jsonNode.get("fx_account") ;
            out.collect(new Tuple5<>(cc_type, fx, fx_account, fx_fx, amount_orig));

        }

    }

    public static class SerializeTuple5toString implements KeyedSerializationSchema<Tuple5<String, String, String, String, Double>> {
        @Override
        public byte[] serializeKey(Tuple5 element) {
            return (null);
        }
        @Override
        public byte[] serializeValue(Tuple5 value) {

            String str = "{"+ value.getField(0).toString()
                    + ":" + value.getField(1).toString()
                    + ":" + value.getField(2).toString()
                    + ":" + value.getField(4).toString() + "}";
            return str.getBytes();
        }
        @Override
        public String getTargetTopic(Tuple5 tuple5) {
            // use always the default topic
            return null;
        }
    }
}