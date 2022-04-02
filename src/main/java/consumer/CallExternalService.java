package consumer;

import com.squareup.okhttp.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * csvStream: msg_id:295, Current_time_is:2021-12-08T09:05:00.440Z
 * <p>
 * run:
 * cd /opt/cloudera/parcels/FLINK &&
 * ./bin/flink run -m yarn-cluster -c consumer.CallExternalService -ynm CallExternalService lib/flink/examples/streaming/streaming-flink-0.5.0.0.jar localhost:9092
 * <p>
 * java -classpath streaming-flink-0.5.0.0.jar consumer.CallExternalService
 *
 * @author Marcel Daeppen
 * @version 2021/12/08 10:12
 */

public class CallExternalService {

    private static final Logger LOG = LoggerFactory.getLogger(CallExternalService.class);
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

        String usecaseId = "CallExternalService";
        String topic = "result_" + usecaseId;

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(1000);


        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, usecaseId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, usecaseId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        Properties propertiesProducer = new Properties();
        propertiesProducer.put(ProducerConfig.CLIENT_ID_CONFIG, usecaseId);
        propertiesProducer.put(ProducerConfig.ACKS_CONFIG, "all");


        KafkaSource<String> eventStream = KafkaSource.<String>builder()
                .setBootstrapServers(brokerURI)
                .setTopics("kafka_simple")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        DataStream<String> transformedStream = env.fromSource(
                        eventStream,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source")
                .map((MapFunction<String, String>) str -> {
                    String[] temp = str.split(",");
                    return String.valueOf(temp[0]).replace("msg_id=", "");
                });

        transformedStream.print(topic + ": ");

        DataStream<String> unorderedHttpResult =
                AsyncDataStream.unorderedWait(
                        // Original stream
                        transformedStream,
                        // The function
                        new HttpRequestFunction(),
                        // Tiemout length
                        5,
                        // Timeout unit
                        TimeUnit.SECONDS);

        unorderedHttpResult.print("unorderedHttpResult" + ": ");


        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokerURI)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(propertiesProducer)
                .build();


        unorderedHttpResult
                .sinkTo(kafkaSink).name("Equipment Kafka Destination");


        // execute program
        JobExecutionResult result = env.execute(usecaseId);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }

    public static class HttpRequestFunction extends RichAsyncFunction<String, String> {

        private transient OkHttpClient client;

        @Override
        public void open(Configuration parameters) {
            client = new OkHttpClient();
        }

        @Override
        public void asyncInvoke(String input, ResultFuture<String> resultFuture) {

            Request request = new Request.Builder()
                    .get()
                    .url("http://localhost:80/api/v3/test/" + input)
                    .build();
            LOG.info("micro-service-request: {}", request.url());
            Call call = client.newCall(request);
            call.enqueue(new Callback() {
                @Override
                public void onFailure(Request request, IOException e) {
                    resultFuture.complete(Collections.emptyList());
                }

                @Override
                public void onResponse(Response response) throws IOException {
                    resultFuture.complete(Collections.singleton(response.body().string()));
                }
            });
        }
    }
}