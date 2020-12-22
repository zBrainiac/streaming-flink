package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * iotStream: 1598101297091, 1, 16bc7e63-95bc-424b-a5d5-69b5bee9644e, Test Message #1186
 *
 * run:
 *    cd /opt/cloudera/parcels/FLINK &&
 *    ./bin/flink run -m yarn-cluster -c consumer.IoTUC7ConsumerCSVSQLLookupCSV -ynm IoTUC7ConsumerCSVSQLLookupCSV lib/flink/examples/streaming/streaming-flink-0.3.0.1.jar localhost:9092 /tmp/lookupHeader.csv
 *
 *    java -classpath streaming-flink-0.3.0.1.jar consumer.IoTUC7ConsumerCSVSQLLookupCSV
 *
 * @author Marcel Daeppen
 * @version 2020/08/22 12:14
 */

public class IoTUC7ConsumerCSVSQLLookupCSV {

    private static final Logger LOG = LoggerFactory.getLogger(IoTUC7ConsumerCSVSQLLookupCSV.class);

    private static String brokerURI = "localhost:9092";
    private static String lookupCSV = "data/lookupHeader.csv";

    public static void main(String[] args) throws Exception {

        if( args.length == 1 ) {
            brokerURI = args[0];
            String parm = "'use customized URI' = " + brokerURI + " & 'use default lookup file location' = " + lookupCSV ;
            LOG.info("Program prop set {}", parm);
        }else if( args.length == 2 ) {
            brokerURI = args[0];
            lookupCSV = args[1];
            String parm = "'use customized URI' = " + brokerURI + " & 'use customized lookup file location' = " + lookupCSV ;
            LOG.info("Program prop set {}", parm);
        }else {
            String parm = "'use default URI' = " + brokerURI + " & 'use default lookup file location' = " + lookupCSV ;
            LOG.info("Program prop set {}", parm);
        }

        String use_case_id = "iot_uc7_Csv_Consumer_SQL_Lookup";
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

        TableSource<?> lookupValues = CsvTableSource
                .builder()
                .path("data/lookup.csv")
                .field("sensor_id", Types.INT)
                .field("location", Types.STRING)
                .fieldDelimiter(",")
                .lineDelimiter("\n")
                .ignoreFirstLine()
                .ignoreParseErrors()
                .build();

        tableEnv.registerTableSource("lookupValues", lookupValues);

        System.out.println("\n CSV Lookup Table Created with Schema: \n");

        //Create a Table Object with the product_sales table.
        Table lookupValuesTable = tableEnv
                .scan("lookupValues");

        lookupValuesTable.printSchema();

        Table lookupTable = tableEnv.scan("lookupValues");

        DataStream<Row> CsvTable = tableEnv.toAppendStream(lookupTable, Row.class);
        CsvTable.print("lookupTable print: ");

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


        String sql = "SELECT * FROM CsvSinkTable, lookupValues WHERE CsvSinkTable.sensor_id =  lookupValues.sensor_id";

        Table iotTable = tableEnv.sqlQuery(sql);
        iotTable.printSchema();

        DataStream<Row> dsRow = tableEnv.toAppendStream(iotTable, Row.class);

        dsRow.print();

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<Row> myProducer = new FlinkKafkaProducer<>(topic,
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
        LOG.info("Job_id {}", jobId);
    }
}
