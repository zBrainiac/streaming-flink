package consumer;

import com.alibaba.fastjson.JSON;
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
 * className: ConsunerFlink.IoTUC4JoinStreams
 * iotStream: {"sensor_ts":1588617762605,"sensor_id":7,"sensor_0":59,"sensor_1":32,"sensor_2":84,"sensor_3":23,"sensor_4":56,"sensor_5":30,"sensor_6":46,"sensor_7":90,"sensor_8":64,"sensor_9":33,"sensor_10":49,"sensor_11":91}
 * csvStream: unixTime :1596022830196, sensor_id :6, id :e78b8a17-527b-4e51-9fdb-577da3207db0, Test Message #198
 *
 * DataStream<String> joinedString = trx.join(fx)
 *
 *
 * run:
 *   cd /opt/cloudera/parcels/FLINK &&
 *    ./bin/flink run -m yarn-cluster -c consumer.IoTUC4JoinStreams -ynm IoTUC4JoinStreams lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar localhost:9092
 *    ./bin/flink run -m yarn-cluster -c consumer.IoTUC4JoinStreams -ynm IoTUC4JoinStreams lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar edge2ai-0.dim.local:9092
 *
 *    java -classpath streaming-flink-0.5.0.0.jar consumer.IoTUC4JoinStreams
 *
 * @author Marcel Daeppen
 * @version 2022/02/06 12:14
 */

public class IoTUC4JoinStreams {

    private static final Logger LOG = LoggerFactory.getLogger(IoTUC4JoinStreams.class);
    private static String brokerURI = "localhost:9092";
    private static final String LOGGMSG = "Program prop set {}";

    public static void main(String[] args) throws Exception {

        if( args.length == 1 ) {
            brokerURI = args[0];
            String parm = "'use program argument parm: URI' = " + brokerURI;
            LOG.info(LOGGMSG, parm);
        }else {
            String parm = "'use default URI' = " + brokerURI;
            LOG.info(LOGGMSG, parm);
        }

        String usecaseid = "IoTUC4JoinStreams";
        String topic = "result_" + usecaseid ;

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


        KafkaSource<String> csvStream = KafkaSource.<String>builder()
                .setBootstrapServers(brokerURI)
                .setTopics("iot_CSV")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        DataStream<JSONObject> csv = env.fromSource(
                        csvStream,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source csv")
                .flatMap(new TokenizerCSV());

        csv.print("csv token: ");


        KafkaSource<String> iotStream = KafkaSource.<String>builder()
                .setBootstrapServers(brokerURI)
                .setTopics("iot")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        DataStream<JSONObject> iot = env.fromSource(
                        iotStream,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source iot")
                .flatMap(new TokenizerIOT());

        iot.print("iot token: ");


        DataStream<String> transformedStream = iot.join(csv)
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

        transformedStream.print("result: ");

        // write the aggregated data stream to a Kafka sink
        KafkaSink<String> kafkaSinkJSON = KafkaSink.<String>builder()
                .setBootstrapServers(brokerURI)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(propertiesProducer)
                .build();

        transformedStream
                .sinkTo(kafkaSinkJSON).name("Equipment Kafka Destination");

        // execute program
        JobExecutionResult result = env.execute(usecaseid);
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
            // System.err.println("iotkey: " + id);
            return id;
        }
    }

    private static class EqualKeySelector implements KeySelector<JSONObject, Integer> {
        @Override
        public Integer getKey(JSONObject value) {
            // select 'sensor_id' from csvStream
            Integer id = (Integer) value.get("sensor_id");
            // for debugging: print out
            // System.err.println("csvkey: " + id);
            return id;
        }
    }

}