package consumer;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * className: ConsunerFlink.KafkaJoin2JsonStreams
 * trxStream: {"timestamp":1565604389166,"shop_id":0,"shop_name":"Ums Eck","cc_type":"Revolut","cc_id":"5179-5212-9764-8013","amount_orig":75.86,"fx":"CHF","fx_account":"CHF"}
 * fxStream: {"timestamp":1565604494202,"fx":"EUR","fx_rate":1.01}
 *
 * DataStream<String> joinedString = trx.join(fx)
 * {"EUR":{"fx":"EUR","fx_rate":0.9,"timestamp":1565604610729},"5130-2220-4900-6727":{"cc_type":"Visa","shop_id":4,"fx":"EUR","amount_orig":86.82,"fx_account":"EUR","cc_id":"5130-2220-4900-6727","shop_name":"Ums Eck","timestamp":1565604610745}}
 *
 *
 * run:
 *    java -classpath streaming-flink-0.1-SNAPSHOT.jar consumer.UC3KafkaJoin2JsonStreams
 *
 * @author Marcel Daeppen
 * @version 2020/04/26 12:14
 */

public class UC3KafkaJoin2JsonStreams {

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

        String use_case_id = "uc3_TrxFxCombined";
        String topic = "result_" + use_case_id ;

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

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
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> fxStream = env.addSource(
                new FlinkKafkaConsumer<>("fxRate", new SimpleStringSchema(), properties));

        //fxStream.print("DataStream - fx");

        DataStream<String> trxStream = env.addSource(
                new FlinkKafkaConsumer<>("cctrx", new SimpleStringSchema(), properties));

        //trxStream.print("DataStream - trx");

        DataStream<JSONObject> trx =
                trxStream.flatMap(new Tokenizer());

        // trx.print("test");

        DataStream<JSONObject> fx =
                fxStream.flatMap(new Tokenizer());

        DataStream<String> joinedString = trx.join(fx)
                .where(new NameKeySelector())
                .equalTo(new EqualKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(2000)))
                .apply((JoinFunction<JSONObject, JSONObject, String>) (first, second) -> {
                    JSONObject joinJson = new JSONObject();
                    joinJson.put("trx", first);
                    joinJson.put("fx", second);

                    // for debugging: print out
           //         System.err.println("trx data: " + first);
           //         System.err.println("fx data: " + second);
                    return joinJson.toJSONString();
                });

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                topic, new SimpleStringSchema(), propertiesProducer);

        joinedString.addSink(myProducer);



        // emit result
        if (parameterTool.has("output")) {
            joinedString.writeAsText(parameterTool.get("output"));
        } else {
            System.err.println("Printing result to stdout. Use --output to specify output path.");
            joinedString.print();
        }

        // execute program
        JobExecutionResult result = env.execute(use_case_id);
        JobID jobId = result.getJobID();
        System.err.println("jobId=" + jobId);
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
}