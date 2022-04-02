package consumer;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class WikiSearch {
    public static void main(String[] args) throws Exception {

        final Logger LOG = LoggerFactory.getLogger(WikiSearch.class);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 //       final HttpRequestFunction httpRequestFunction = new HttpRequestFunction("https://en.wikipedia.org/w/api.php?action=query&list=search&prop=info&inprop=url&utf8=&format=json&origin=*&srlimit=20&srsearch=",false);


        String usecaseid = "IoTUC1CountEventsPerSensorId";
        String topic = "result_" + usecaseid;

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, usecaseid);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, usecaseid);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringConsumerInterceptor");

        KafkaSource<String> eventStream = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")     // TODO
                .setTopics("iot")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

/*
        DataStream<String> resultStream = AsyncDataStream.orderedWait(
                // Original stream
                eventStream,
                // The function
                httpRequestFunction(),
                // Tiemout length
                5,
                // Timeout unit
                TimeUnit.SECONDS
        );
*/

        // execute program
        JobExecutionResult result = env.execute(usecaseid);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }

}