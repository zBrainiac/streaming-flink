package consumer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * iotStream: 1598101297091, 1, 16bc7e63-95bc-424b-a5d5-69b5bee9644e, Test Message #1186
 * <p>
 * run:
 * cd /opt/cloudera/parcels/FLINK &&
 * ./bin/flink run -m yarn-cluster -c consumer.IoTUC9SQLLookupJSON -ynm IoTUC9SQLLookupJSON lib/flink/examples/streaming/streaming-flink-0.4.0.0.jar localhost:9092
 * ./bin/flink run -m yarn-cluster -c consumer.IoTUC9SQLLookupJSON -ynm IoTUC9SQLLookupJSON lib/flink/examples/streaming/streaming-flink-0.4.0.0.jar edge2ai-1.dim.local:9092 /tmp/lookup.csv
 * java -classpath streaming-flink-0.4.0.0.jar consumer.IoTUC9SQLLookupJSON edge2ai-1.dim.local:9092
 *
 * @author Marcel Daeppen
 * @version 2020/08/24 12:14
 */

public class IoTUC9SQLLookupJSON {

    private static final Logger LOG = LoggerFactory.getLogger(IoTUC9SQLLookupJSON.class);
    private static final String LOGGERMSG = "Program prop set {}";
    private static String brokerURI = "localhost:9092";
    private static String lookupCSV = "data/lookup.csv";

    public static void main(String[] args) throws Exception {

        if (args.length == 1) {
            brokerURI = args[0];
            String parm = "'use customized URI' = " + brokerURI + " & 'use default lookup file location' = " + lookupCSV;
            LOG.info(LOGGERMSG, parm);
        } else if (args.length == 2) {
            brokerURI = args[0];
            lookupCSV = args[1];
            String parm = "'use customized URI' = " + brokerURI + " & 'use customized lookup file location' = " + lookupCSV;
            LOG.info(LOGGERMSG, parm);
        } else {
            String parm = "'use default URI' = " + brokerURI + " & 'use default lookup file location' = " + lookupCSV;
            LOG.info(LOGGERMSG, parm);
        }

        String use_case_id = "iot_uc9_SQL_Lookup_JSON";
        String topic = "result_" + use_case_id;

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        Properties propertiesProducer = new Properties();
        propertiesProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        propertiesProducer.put(ProducerConfig.CLIENT_ID_CONFIG, use_case_id);
        propertiesProducer.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");


        final Schema schemaTableIoT = new Schema()
                .field("sensor_ts", DataTypes.BIGINT())
                .field("sensor_id", DataTypes.INT())
                .field("sensor_0", DataTypes.INT())
                .field("sensor_1", DataTypes.INT())
                .field("sensor_2", DataTypes.INT())
                .field("sensor_3", DataTypes.INT())
                .field("sensor_4", DataTypes.INT())
                .field("sensor_5", DataTypes.INT())
                .field("sensor_6", DataTypes.INT())
                .field("sensor_7", DataTypes.INT())
                .field("sensor_8", DataTypes.INT())
                .field("sensor_9", DataTypes.INT())
                .field("sensor_10", DataTypes.INT())
                .field("sensor_11", DataTypes.INT());

        final Schema schema_lookupValues = new Schema()
                .field("sensor_id", DataTypes.INT())
                .field("city", DataTypes.STRING())
                .field("lat", DataTypes.DOUBLE())
                .field("lon", DataTypes.DOUBLE());

        tableEnv.connect(new FileSystem().path(lookupCSV))
                .withFormat(new Csv().fieldDelimiter(',').deriveSchema())
                .withSchema(schema_lookupValues)
                .createTemporaryTable("lookupValues");


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
                        .version("universal")
                        .topic("iot")
                        .startFromLatest()
                        .property("bootstrap.servers", brokerURI)
                        .property("group.id", use_case_id)
                        .property("INTERCEPTOR_CLASSES_CONFIG", "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringConsumerInterceptor")
        )
                .withFormat(new Json())
                .withSchema(schemaTableIoT)
                .createTemporaryTable("JSONSinkTable");


        String sql = "SELECT " +
                "  JSONSinkTable.sensor_ts" +
                ", JSONSinkTable.sensor_id" +
                ", JSONSinkTable.sensor_0" +
                ", JSONSinkTable.sensor_2" +
                ", JSONSinkTable.sensor_3" +
                ", lookupValues.city" +
                ", lookupValues.lat" +
                ", lookupValues.lon " +
                "FROM JSONSinkTable, lookupValues " +
                "WHERE JSONSinkTable.sensor_id = lookupValues.sensor_id";

        Table iotTable = tableEnv.sqlQuery(sql);
        iotTable.printSchema();

        DataStream<Row> aggStream = tableEnv.toAppendStream(iotTable, Row.class);

        aggStream.print("sql result: ");

        // write the aggregated data stream to a Kafka sink
        FlinkKafkaProducer<Row> myProducer = new FlinkKafkaProducer<>(
                topic, new SerializeSum2String(), propertiesProducer);

        aggStream.addSink(myProducer);


        // execute program
        JobExecutionResult result = env.execute(use_case_id);
        JobID jobId = result.getJobID();
        LOG.info("Job_id {}", jobId);
    }

    public static class SerializeSum2String implements KeyedSerializationSchema<Row> {
        @Override
        public byte[] serializeKey(Row element) {
            return (null);
        }

        @Override
        public byte[] serializeValue(Row value) {
            String str = "{"
                    + "\"type\"" + ":" + "\"ok\""
                    + "," + "\"subtype\"" + ":" + "\"message enrichment\""
                    + "," + "\"sensor_ts\"" + ":" + value.getField(0)
                    + "," + "\"sensor_id\"" + ":" + value.getField(1)
                    + "," + "\"sensor_0\"" + ":" + value.getField(2)
                    + "," + "\"sensor_1\"" + ":" + value.getField(3)
                    + "," + "\"sensor_3\"" + ":" + value.getField(4)
                    + "," + "\"city\"" + ":" + "\"" + value.getField(5).toString().trim() + "\""
                    + "," + "\"lat\"" + ":" + value.getField(6)
                    + "," + "\"long\"" + ":" + value.getField(7) + "}";
            return str.getBytes();
        }

        @Override
        public String getTargetTopic(Row row) {
            return null;
        }

    }

}
