package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


/**
 * iotStream: 1598101297091, 1, 16bc7e63-95bc-424b-a5d5-69b5bee9644e, Test Message #1186
 *
 * run:
 *    cd /opt/cloudera/parcels/FLINK &&
 *    ./bin/flink run -m yarn-cluster -c consumer.IoTCsvConsumerSQLFilter -ynm IoTCsvConsumerSQLFilter lib/flink/examples/streaming/streaming-flink-0.3.0.1.jar localhost:9092
 *
 *    java -classpath streaming-flink-0.3.0.1.jar consumer.IoTCsvConsumerSQLFilter
 *
 * @author Marcel Daeppen
 * @version 2020/08/22 12:14
 */

public class IoTCsvConsumerSQLFilter {

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

        String use_case_id = "iot_SQL_Filter";
        String topic = "result_" + use_case_id;

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);


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


        final Schema schemaTableIoT = new Schema()
                .field("sensor_ts", DataTypes.BIGINT())
                .field("sensor_id", DataTypes.INT())
                .field("uuid", DataTypes.STRING())
                .field("text", DataTypes.STRING());

        tableEnv.connect(
                new Kafka()
                        .version("universal")    // required: valid connector versions are
                        .topic("iot_CSV")       // required: topic name from which the table is read
                        .startFromLatest()
                        .property("bootstrap.servers", "localhost:9092")
                        .property("group.id", "testGroup")
        )
                .withFormat(new Csv().fieldDelimiter(','))
                .withSchema(schemaTableIoT)
                .createTemporaryTable("CsvSinkTable");


        String sql = "SELECT * FROM CsvSinkTable WHERE sensor_id = 3";

        Table iotTable = tableEnv.sqlQuery(sql);
        iotTable.printSchema();

        DataStream<Row> dsRow = tableEnv.toAppendStream(iotTable, Row.class);

        dsRow.print();

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer myProducer = new FlinkKafkaProducer<>(topic,
                (KafkaSerializationSchema<Row>) (element, timestamp) -> new ProducerRecord<byte[], byte[]>(topic,
                        (element.getField(3)).toString().getBytes(),
                        (element.toString()).getBytes()
                ),
                propertiesProducer,
                FlinkKafkaProducer.Semantic.NONE);

        dsRow.addSink(myProducer);

        // execute program
        JobExecutionResult result = env.execute(use_case_id);
        JobID jobId = result.getJobID();
        System.err.println("jobId=" + jobId);
    }

    public static class SerializeSum2String implements KeyedSerializationSchema<Tuple4<Long, Integer, String, String>> {
        @Override
        public byte[] serializeKey(Tuple4 element) {
            return (null);
        }
        @Override
        public byte[] serializeValue(Tuple4 value) {

            String str = "{"
                    + "\"type\"" + ":" + "\"ok\""
                    + "," + "\"subtype\"" + ":" + "\"filter sensor_id #3 \""
                    + "," + "\"sensor_ts\"" + ":" + value.getField(0).toString()
                    + "," + "\"uuid\"" + ":" + value.getField(2).toString()
                    + "," + "\"msg\"" + ":" + value.getField(3)  + "}";
            return str.getBytes();
        }

        @Override
        public String getTargetTopic(Tuple4<Long, Integer, String, String> longIntegerStringStringTuple4) {
            return null;
        }
    }
}
