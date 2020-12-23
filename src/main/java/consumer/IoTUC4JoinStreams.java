package consumer;

import com.alibaba.fastjson.JSON;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * className: ConsunerFlink.IoTUC4JoinStreams
 * iotStream: {"sensor_ts":1588617762605,"sensor_id":7,"sensor_0":59,"sensor_1":32,"sensor_2":84,"sensor_3":23,"sensor_4":56,"sensor_5":30,"sensor_6":46,"sensor_7":90,"sensor_8":64,"sensor_9":33,"sensor_10":49,"sensor_11":91}
 * csvStream: unixTime :1596022830196, sensor_id :6, id :e78b8a17-527b-4e51-9fdb-577da3207db0, Test Message #198
 *
 * DataStream<String> joinedString = trx.join(fx)
 *
 *
 * run:
 *    java -classpath streaming-flink-0.3.1.0.jar consumer.IoTUC4JoinStreams
 *
 * @author Marcel Daeppen
 * @version 2020/07/29 20:14
 */

public class IoTUC4JoinStreams {

    private static final Logger LOG = LoggerFactory.getLogger(FSIUC1KafkaCountTrxPerShop.class);
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

        String use_case_id = "iot-Join_IoTandCSV";
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

        DataStream<String> csvStream = env.addSource(
                new FlinkKafkaConsumer<>("iot_CSV", new SimpleStringSchema(), properties));
        /* csvStream.print("csv stream: "); */

        DataStream<JSONObject> csv =
                csvStream.flatMap(new TokenizerCSV());
        csv.print("csv token: ");


        DataStream<String> iotStream = env.addSource(
                new FlinkKafkaConsumer<>("iot", new SimpleStringSchema(), properties));
        /* iotStream.print("iot stream:"); */

        DataStream<JSONObject> iot =
                iotStream.flatMap(new TokenizerIOT());
        iot.print("iot token: ");


        DataStream<String> joinedString = iot.join(csv)
                .where(new NameKeySelector())
                .equalTo(new EqualKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10000)))
                .apply((first, second) -> {
                    JSONObject joinJson = new JSONObject();
                    joinJson.put("iot", first);
                    joinJson.put("csv", second);

                    // for debugging: print out
                    //       System.err.println("iot data: " + first);
                    //       System.err.println("csv data: " + second);
                    return joinJson.toJSONString();
                });

        joinedString.print("result: ");

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                topic, new SimpleStringSchema(), propertiesProducer);

        joinedString.addSink(myProducer);

        // execute program
        JobExecutionResult result = env.execute(use_case_id);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }

    public static final class TokenizerCSV implements FlatMapFunction<String, JSONObject> {
        // 1597569761712, 1, 2fe3ab93-c0b3-4c1b-811c-cc4f9f832734, Test Message #364
        @Override
        public void flatMap(String value, Collector<JSONObject> out) {
            try {
                String[] token = value.split(",");

                String jsonStr = "{"
                        + "\"unixtsd\"" + ":" + token[0]
                        + "," + "\"sensor_id\"" + ":" + token[1]
                        + "," + "\"uuid\"" + ":" + "\"" + token[2] + "\""
                        + "," + "\"message\"" + ":" + "\""+ token[3] + "\"" + "}";
                JSONObject jsonObject  = JSON.parseObject(jsonStr);
                // System.out.println(jsonObject);
                // System.out.println(jsonObject.get("uuid"));

                out.collect(jsonObject);

            } catch (Exception ex) {
                        System.err.println("ex: " + value +ex);
            }
        }
    }

    public static final class TokenizerIOT implements FlatMapFunction<String, JSONObject> {
        @Override
        public void flatMap(String value, Collector<JSONObject> out) {
            try {
                /* System.err.println("try"); */
                JSONObject json = JSONObject.parseObject(value);
                out.collect(json);
            } catch (Exception ex) {
                /* System.err.println("ex: " + value +ex); */
            }
        }
    }

    private static class NameKeySelector implements KeySelector<JSONObject, Integer> {
        @Override
        public Integer getKey(JSONObject value) {
            // select 'sensor_id' from csvStream
            int id = (int) value.get("sensor_id");
            // for debugging: print out
            System.err.println("iotkey: " + id);
            return id;
        }
    }

    private static class EqualKeySelector implements KeySelector<JSONObject, Integer> {
        @Override
        public Integer getKey(JSONObject value) {
            // select 'sensor_id' from csvStream
            Integer id = (Integer) value.get("sensor_id");
            // for debugging: print out
            System.err.println("csvkey: " + id);
            return id;
        }
    }

}