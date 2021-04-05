package consumer;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * className: ConsunerFlink.KafkaJoin2JsonStreams
 * trxStream: {"timestamp":1565604389166,"shop_id":0,"game_name":"Ums Eck","cc_type":"Revolut","cc_id":"5179-5212-9764-8013","amount_orig":75.86,"fx":"CHF","fx_account":"CHF"}
 * fxStream: {"timestamp":1565604494202,"fx":"EUR","fx_rate":1.01}
 *
 * DataStream<String> joinedString = trx.join(fx)
 * {"EUR":{"fx":"EUR","fx_rate":0.9,"timestamp":1565604610729},"5130-2220-4900-6727":{"cc_type":"Visa","shop_id":4,"fx":"EUR","amount_orig":86.82,"fx_account":"EUR","cc_id":"5130-2220-4900-6727","game_name":"Ums Eck","timestamp":1565604610745}}
 *
 *
 * run:
 *    java -classpath streaming-flink-0.4.0.0.jar consumer.FSIUC3KafkaJoin2JsonStreams
 *
 * @author Marcel Daeppen
 * @version 2020/04/26 12:14
 */

public class KafkaFootballTicketsTRXJoin2JsonStreamsDiffOut {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFootballTicketsTRXJoin2JsonStreamsDiffOut.class);
    private static String brokerURI = "localhost:9092";
    private static final String LOGGERMSG = "Program prop set {}";
    private static DataStream<String> joinedString;
    private DataStream<String> joinedString1;

    public static void main(String[] args) throws Exception {

        if( args.length == 1 ) {
            brokerURI = args[0];
            String parm = "'use program argument parm: URI' = " + brokerURI;
            LOG.info(LOGGERMSG, parm);
        }else {
            String parm = "'use default URI' = " + brokerURI;
            LOG.info(LOGGERMSG, parm);
        }

        String use_case_id = "Football-uc1-TicketsTRXJoin2JsonStreams";
        String topicJSON = "result_" + use_case_id + "_json" ;
        String topicCSV = "result_" + use_case_id + "_csv" ;
        String topicFlatJSON = "result_" + use_case_id + "_flatjson" ;

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        //it is necessary to use IngestionTime, not EventTime. during my running this program

        DataStream<String> fxStream = env.addSource(
                new FlinkKafkaConsumer<>("fxRate", new SimpleStringSchema(), properties));

        DataStream<String> trxStream = env.addSource(
                new FlinkKafkaConsumer<>("FootballTicketsTRX", new SimpleStringSchema(), properties));

        //fxStream.print("DataStream - fx");

        //trxStream.print("DataStream - trx");

        DataStream<JSONObject> trx =
                trxStream.flatMap(new Tokenizer());

        // trx.print("Test");

        DataStream<JSONObject> fx =
                fxStream.flatMap(new Tokenizer());
        
        DataStream<String> joinedString = trx.join(fx)
                .where(new NameKeySelector())
                .equalTo(new EqualKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(2000)))
                .apply((first, second) -> {
                    JSONObject joinJson = new JSONObject();
                    joinJson.put("trx", first);
                    joinJson.put("fx", second);

                    // for debugging: print out
           //         System.err.println("trx data: " + first);
           //         System.err.println("fx data: " + second);
                    return joinJson.toJSONString();
                });

        FlinkKafkaProducer<String> myKafkaProducerJSON = new FlinkKafkaProducer<>(
                topicJSON,
                new SimpleStringSchema(),
                propertiesProducer);

        joinedString.addSink(myKafkaProducerJSON);

        DataStream <String> csvStream = joinedString
                .flatMap(new JSON2CSVDeserializer());

        // write the aggregated data stream to a Kafka sink format CSV
        FlinkKafkaProducer<String> myKafkaProducerCSV = new FlinkKafkaProducer<>(
                topicCSV,
                new SimpleStringSchema(),
                propertiesProducer);

        csvStream.addSink(myKafkaProducerCSV);


        DataStream <String> flatJSONStream = joinedString
                .flatMap(new JSON2FlatJSONDeserializer());


        // write the aggregated data stream to a Kafka sink format CSV
        FlinkKafkaProducer<String> myKafkaProducerFlatJSON = new FlinkKafkaProducer<>(
                topicFlatJSON,
                new SimpleStringSchema(),
                propertiesProducer);

        flatJSONStream.addSink(myKafkaProducerFlatJSON);

        flatJSONStream.print("flatJSONStream: ");



        // execute program
        JobExecutionResult result = env.execute(use_case_id);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }


    public static final class Tokenizer implements FlatMapFunction<String, JSONObject> {
        @Override
        public void flatMap(String value, Collector<JSONObject> out) {
            try {
                JSONObject json = JSONObject.parseObject(value);
                out.collect(json);
            } catch (Exception ex) {
                System.err.println(value + ex);
            }
        }
    }

    private static class NameKeySelector implements KeySelector<JSONObject, String> {
        @Override
        public String getKey(JSONObject value) {
            // select fx && fx_account from fxStream
            final String str = (String) value.get("fx") + "_" + (String) value.get("fx_account");
// for debugging: print out
//            System.err.println("trx: " + str);
            return str;
        }
    }

    private static class EqualKeySelector implements KeySelector<JSONObject, String> {
        @Override
        public String getKey(JSONObject value) {
            // select fx && fx_target from fxStream
            final String str = (String) value.get("fx") + "_" + (String) value.get("fx_target");
// for debugging: print out
//           System.err.println("fx: " + str);
            return str;
        }
    }

    public static class JSON2CSVDeserializer implements FlatMapFunction<String, String> {
        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String value, Collector<String> collector) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            // get game_name AND fx from JSONObject
            String result = new StringBuilder()
                    .append(jsonNode.get("trx").get("timestamp").toString())
                    .append(",")
                    .append(jsonNode.get("trx").get("cc_id").toString())
                    .append(",")
                    .append(jsonNode.get("trx").get("cc_type").toString())
                    .append(",")
                    .append(jsonNode.get("trx").get("shop_id").toString())
                    .append(",")
                    .append(jsonNode.get("trx").get("game_name").toString())
                    .append(",")
                    .append(jsonNode.get("trx").get("amount_orig").toString())
                    .append(",")
                    .append(jsonNode.get("trx").get("fx").toString())
                    .append(",")
                    .append(jsonNode.get("fx").get("fx_target").toString())
                    .append(",")
                    .append(jsonNode.get("fx").get("timestamp").toString())
                    .append(",")
                    .append(jsonNode.get("fx").get("fx_rate").toString())
                    .toString()
                    .replace("\"", "");
            collector.collect(result);
        }

    }

    public static class JSON2FlatJSONDeserializer implements FlatMapFunction<String, String> {
        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String value, Collector<String> collector) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            // get game_name AND fx from JSONObject
            String result = new StringBuilder()
                    .append("{\"timestamp\":")
                    .append(jsonNode.get("trx").get("timestamp").toString())
                    .append(",\"cc_id\":")
                    .append(jsonNode.get("trx").get("cc_id").toString())
                    .append(",\"cc_type\":")
                    .append(jsonNode.get("trx").get("cc_type").toString())
                    .append(",\"shop_id\":")
                    .append(jsonNode.get("trx").get("shop_id").toString())
                    .append(",\"game_name\":")
                    .append(jsonNode.get("trx").get("game_name").toString())
                    .append(",\"amount_orig\":")
                    .append(jsonNode.get("trx").get("amount_orig").toString())
                    .append(",\"fx\":")
                    .append(jsonNode.get("trx").get("fx").toString())
                    .append(",\"fx_target\":")
                    .append(jsonNode.get("fx").get("fx_target").toString())
                    .append(",\"timestamp_fx\":")
                    .append(jsonNode.get("fx").get("timestamp").toString())
                    .append(",\"fx_rate\":")
                    .append(jsonNode.get("fx").get("fx_rate").toString())
                    .append("}")
                    .toString();
            collector.collect(result);
        }

    }


}