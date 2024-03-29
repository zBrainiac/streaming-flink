package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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
 *    ./bin/flink run -m yarn-cluster -c consumer.OPCUC1NoiseCanceller -ynm OPCUC1NoiseCanceller lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar localhost:9092
 *
 *    java -classpath streaming-flink-0.5.0.0.jar consumer.OPCUC1NoiseCanceller
 *
 * @author Marcel Daeppen
 * @version 2022/02/06 12:14
 */

public class OPCUC1NoiseCanceller {

    private static final Logger LOG = LoggerFactory.getLogger(OPCUC1NoiseCanceller.class);
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

        String usecaseid = "OPCUC1NoiseCanceller";
        String topic = "result_" + usecaseid;

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

        KafkaSource<String> eventStream = KafkaSource.<String>builder()
                .setBootstrapServers(brokerURI)
                .setTopics("opc")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        DataStream<Tuple4<String, String, Double, Integer>> transformedStream = env.fromSource(
                eventStream,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source")
                .flatMap(new OPCJSONDeserializer())
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

        transformedStream.print(topic + ": ");

        // write the aggregated data stream to a Kafka sink
        transformedStream.print(topic + ": ");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokerURI)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(propertiesProducer)
                .build();

        transformedStream.map((MapFunction<Tuple4<String, String, Double, Integer>, String>) s -> "{"
                        + "\"type\"" + ":" + "\"aggregated opc stream over 10sec.\""
                        + "," + "\"ts\"" + ":" + "\"" + s.f0 + "\""
                        + "," + "\"tagname\"" + ":" + "\"" + s.f1 + "\""
                        + "," + "\"value\"" + ":" + "\"" + s.f2 + "\"" + "}")
                .sinkTo(kafkaSink).name("Equipment Kafka Destination");

        // execute program
        JobExecutionResult result = env.execute(usecaseid);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
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