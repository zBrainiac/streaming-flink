package consumer;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * className: ConsunerFlink.KafkaJoin2JsonStreams
 * trxStream: {"timestamp":1565604389166,"shop_id":0,"shop_name":"Ums Eck","cc_type":"Revolut","cc_id":"5179-5212-9764-8013","amount_orig":75.86,"fx":"CHF","fx_account":"CHF"}
 * fxStream: {"timestamp":1565604494202,"fx":"EUR","fx_rate":1.01}
 * <p>
 * DataStream<String> joinedString = trx.join(fx)
 * {"EUR":{"fx":"EUR","fx_rate":0.9,"timestamp":1565604610729},"5130-2220-4900-6727":{"cc_type":"Visa","shop_id":4,"fx":"EUR","amount_orig":86.82,"fx_account":"EUR","cc_id":"5130-2220-4900-6727","shop_name":"Ums Eck","timestamp":1565604610745}}
 * <p>
 * <p>
 * run:
 * java -classpath streaming-flink-0.5.0.0.jar consumer.FSIUC3KafkaJoin2JsonStreams
 *
 * @author Marcel Daeppen
 * @version 2022/02/06 12:14
 */

public class FSIUC4Join2JsonStreamsdiffOut {

    private static final Logger LOG = LoggerFactory.getLogger(FSIUC4Join2JsonStreamsdiffOut.class);
    private static final String LOGGMSG = "Program prop set {}";
    private static String brokerURI = "localhost:9092";

    public static void main(String[] args) throws Exception {

        if (args.length == 1) {
            brokerURI = args[0];
            String parm = "'use program argument parm: URI' = " + brokerURI;
            LOG.info(LOGGMSG, parm);
        } else {
            String parm = "'use default URI' = " + brokerURI;
            LOG.info(LOGGMSG, parm);
        }

        String usecaseid = "FSIUC4CountTrxFxCombinedDiffOut";
        String topicJSON = "result_" + usecaseid + "_json" ;
        String topicCSV = "result_" + usecaseid + "_csv" ;
        String topicFlatJSON = "result_" + usecaseid + "_flatjson" ;

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, usecaseid);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, usecaseid);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringConsumerInterceptor");

        Properties propertiesProducer = new Properties();
        propertiesProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        propertiesProducer.put(ProducerConfig.CLIENT_ID_CONFIG, usecaseid);
        propertiesProducer.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");


        KafkaSource<String> fxStream = KafkaSource.<String>builder()
                .setBootstrapServers(brokerURI)
                .setTopics("fxRate")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        //fxStream.print("DataStream - fx");

        KafkaSource<String> trxStream = KafkaSource.<String>builder()
                .setBootstrapServers(brokerURI)
                .setTopics("cctrx")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        //trxStream.print("DataStream - trx");

        DataStream<JSONObject> trx = env.fromSource(
                        trxStream,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source credit card trx")
                .flatMap(new Tokenizer());

        //trx.print("Test");

        DataStream<JSONObject> fx = env.fromSource(
                        fxStream,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source credit card trx")
                .flatMap(new Tokenizer());


        //fx.print("Test");

        DataStream<String> transformedStream = trx.join(fx)
                .where(new NameKeySelector())
                .equalTo(new EqualKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(2000)))
                .apply((first, second) -> {
                    JSONObject joinJson = new JSONObject();
                    joinJson.put("trx", first);
                    joinJson.put("fx", second);

                    // for debugging: print out
//                    System.err.println("trx data: " + first)
//                    System.err.println("fx data: " + second);
                    return joinJson.toJSONString();
                });

        // write the aggregated data stream to a Kafka sink
        transformedStream.print("internal" + ": ");

        // Nested JSON output
        KafkaSink<String> kafkaSinkJSON = KafkaSink.<String>builder()
                .setBootstrapServers(brokerURI)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicJSON)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(propertiesProducer)
                .build();

        transformedStream
                .sinkTo(kafkaSinkJSON).name("Equipment Kafka Destination");


        // Flat JSON output
        KafkaSink<String> kafkaSinkFlatJSON = KafkaSink.<String>builder()
                .setBootstrapServers(brokerURI)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicFlatJSON)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(propertiesProducer)
                .build();

        transformedStream
                .flatMap(new JSON2FlatJSONDeserializer())
                .sinkTo(kafkaSinkFlatJSON).name("Equipment Kafka Destination");

        // CSV output
        KafkaSink<String> kafkaSinkCSV = KafkaSink.<String>builder()
                .setBootstrapServers(brokerURI)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicCSV)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(propertiesProducer)
                .build();

        transformedStream
                .flatMap(new JSON2CSVDeserializer())
                .sinkTo(kafkaSinkCSV).name("Equipment Kafka Destination");

        // execute program
        JobExecutionResult result = env.execute(usecaseid);
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
                LOG.error(value + ex);
            }
        }
    }

    private static class NameKeySelector implements KeySelector<JSONObject, String> {
        @Override
        public String getKey(JSONObject value) {
            // select fx && fx_account from fxStream
            final String str = value.get("fx") + "_" + value.get("fx_account");
// for debugging: print out
//            LOG.error("trx: " + str);
            return str;
        }
    }

    private static class EqualKeySelector implements KeySelector<JSONObject, String> {
        @Override
        public String getKey(JSONObject value) {
            // select fx && fx_target from fxStream
            final String str = value.get("fx") + "_" + value.get("fx_target");
// for debugging: print out
//            LOG.error("fx: " + str);
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

            // get shop_name AND fx from JSONObject
            String result = new StringBuilder()
                    .append(jsonNode.get("trx").get("timestamp").toString())
                    .append(",")
                    .append(jsonNode.get("trx").get("cc_id").toString())
                    .append(",")
                    .append(jsonNode.get("trx").get("cc_type").toString())
                    .append(",")
                    .append(jsonNode.get("trx").get("shop_id").toString())
                    .append(",")
                    .append(jsonNode.get("trx").get("shop_name").toString())
                    .append(",")
                    .append(jsonNode.get("trx").get("amount_orig").toString())
                    .append(",")
                    .append(jsonNode.get("trx").get("fx").toString())
                    .append(",")
                    .append(jsonNode.get("fx").get("fx_target").toString())
                    .append(",")
                    .append(jsonNode.get("fx").get("fx_ts").toString())
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

            // get shop_name AND fx from JSONObject
            String result = new StringBuilder()
                    .append("{\"timestamp\":")
                    .append(jsonNode.get("trx").get("timestamp").toString())
                    .append(",\"cc_id\":")
                    .append(jsonNode.get("trx").get("cc_id").toString())
                    .append(",\"cc_type\":")
                    .append(jsonNode.get("trx").get("cc_type").toString())
                    .append(",\"shop_id\":")
                    .append(jsonNode.get("trx").get("shop_id").toString())
                    .append(",\"shop_name\":")
                    .append(jsonNode.get("trx").get("shop_name").toString())
                    .append(",\"amount_orig\":")
                    .append(jsonNode.get("trx").get("amount_orig").toString())
                    .append(",\"fx\":")
                    .append(jsonNode.get("trx").get("fx").toString())
                    .append(",\"fx_target\":")
                    .append(jsonNode.get("fx").get("fx_target").toString())
                    .append(",\"timestamp_fx\":")
                    .append(jsonNode.get("fx").get("fx_ts").toString())
                    .append(",\"fx_rate\":")
                    .append(jsonNode.get("fx").get("fx_rate").toString())
                    .append("}")
                    .toString();
            collector.collect(result);
        }

    }
}