package consumer;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
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
 * Input streams
 *    TrafficCounterRaw: {"sensor_ts":1596956979295,"sensor_id":8,"probability":50,"sensor_x":47,"typ":"LKW","light":false,"license_plate":"DE 483-5849","toll_typ":"10-day"}
 *    TrafficIOTRaw: {"sensor_ts":1597076764377,"sensor_id":4,"temp":15,"rain_level":0,"visibility_level":0}
 *
 * DataStream<String> joinedString
 *   { "traffic":{ "sensor_ts":1596952893254, "sensor_id":1, "license_plate":"AT 448-3946", "toll_typ":"2-month", "probability":96, "sensor_x":76, "typ":"LKW", "light":false }, "iot":{ "sensor_ts":1597138335247, "sensor_id":1, "temp":10, "rain_level":2, "visibility_level":2 } }
 *
 *
 * run:
 *   cd /opt/cloudera/parcels/FLINK &&
 *   ./bin/flink run -m yarn-cluster -c consumer.TrafficUC5Join -ynm TrafficUC5Join lib/flink/examples/streaming/streaming-flink-0.3.0.1.jar localhost:9092
 *   ./bin/flink run -m yarn-cluster -c consumer.TrafficUC5Join -ynm TrafficUC5Join lib/flink/examples/streaming/streaming-flink-0.3.0.1.jar edge2ai-1.dim.local:9092
 *   java -classpath streaming-flink-0.3.0.1.jar consumer.TrafficUC5Join
 *
 * @author Marcel Daeppen
 * @version 2020/08/12 14:54
 */

public class TrafficUC5Join {

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

        String use_case_id = "Traffic_UC5_Join";
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

        //it is necessary to use IngestionTime, not EventTime. during my running this program
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> trafficStream = env.addSource(
                new FlinkKafkaConsumer<>("TrafficCounterRaw", new SimpleStringSchema(), properties));

        trafficStream.print("DataStream - trafficStream");

        DataStream<String> iotStream = env.addSource(
                new FlinkKafkaConsumer<>("TrafficIOTRaw", new SimpleStringSchema(), properties));

        iotStream.print("DataStream - iotStream");

        DataStream<JSONObject> iot =
                iotStream.flatMap(new Tokenizer());

        iot.print("test iot: ");

        DataStream<JSONObject> traffic =
                trafficStream.flatMap(new Tokenizer());
        traffic.print("test fx: ");

        DataStream<String> joinedString = traffic.join(iot)
                .where(new NameKeySelector())
                .equalTo(new EqualKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10000)))
                .apply((JoinFunction<JSONObject, JSONObject, String>) (first, second) -> {
                    JSONObject joinJson = new JSONObject();
                    joinJson.put("traffic", first);
                    joinJson.put("iot", second);

                    // for debugging: print out
                             System.err.println("traffic data: " + first);
                             System.err.println("iot data: " + second);
                    return joinJson.toJSONString();
                });

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                topic, new SimpleStringSchema(), propertiesProducer);

        joinedString.addSink(myProducer);

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

    private static class NameKeySelector implements KeySelector<JSONObject, Integer> {
        @Override
        public Integer getKey(JSONObject value) {
            // select fx && fx_account from trafficStream
            final int str = (int) value.get("sensor_id");
// for debugging: print out
            System.err.println("matchkey traffic: " + str);
            return str;
        }
    }

    private static class EqualKeySelector implements KeySelector<JSONObject, Integer> {
        @Override
        public Integer getKey(JSONObject value) {
            // select fx && fx_target from iotStream
            final int str = (int) value.get("sensor_id");
// for debugging: print out
           System.err.println("matchkey iot: " + str);
            return str;
        }
    }
}