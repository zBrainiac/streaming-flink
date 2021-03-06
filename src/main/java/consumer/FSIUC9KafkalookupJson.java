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
 *
 * run:
 *    cd /opt/cloudera/parcels/FLINK &&
 *    ./bin/flink run -m yarn-cluster -c consumer.FSIUC9KafkalookupJson -ynm FSIUC9KafkalookupJson lib/flink/examples/streaming/streaming-flink-0.4.0.0.jar localhost:9092
 *    ./bin/flink run -m yarn-cluster -c consumer.FSIUC9KafkalookupJson -ynm FSIUC9KafkalookupJson lib/flink/examples/streaming/streaming-flink-0.4.0.0.jar edge2ai-1.dim.local:9092 /tmp/lookupHeader.csv
 *    java -classpath streaming-flink-0.4.0.0.jar consumer.FSIUC9KafkalookupJson edge2ai-1.dim.local:9092
 *
 * @author Marcel Daeppen
 * @version 2020/08/24 12:14
 */

public class FSIUC9KafkalookupJson {

    private static final Logger LOG = LoggerFactory.getLogger(FSIUC9KafkalookupJson.class);

    private static String brokerURI = "localhost:9092";
    private static String lookupCSV = "data/lookup.csv";
    private static final String LOGGERMSG = "Program prop set {}";

    public static void main(String[] args) throws Exception {

        if( args.length == 1 ) {
            brokerURI = args[0];
            String parm = "'use customized URI' = " + brokerURI + " & 'use default lookup file location' = " + lookupCSV ;
            LOG.info(LOGGERMSG, parm);
        }else if( args.length == 2 ) {
            brokerURI = args[0];
            lookupCSV = args[1];
            String parm = "'use customized URI' = " + brokerURI + " & 'use customized lookup file location' = " + lookupCSV ;
            LOG.info(LOGGERMSG, parm);
        }else {
            String parm = "'use default URI' = " + brokerURI + " & 'use default lookup file location' = " + lookupCSV ;
            LOG.info(LOGGERMSG, parm);
        }

        String use_case_id = "fsi_uc9_SQL_Lookup_JSON";
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
                .field("timestamp", DataTypes.STRING())
                .field("cc_id", DataTypes.STRING())
                .field("cc_type", DataTypes.STRING())
                .field("shop_id", DataTypes.INT())
                .field("shop_name", DataTypes.STRING())
                .field("fx", DataTypes.STRING())
                .field("fx_account", DataTypes.STRING())
                .field("amount_orig", DataTypes.STRING());


        final Schema schema_lookupValues = new Schema()
                .field("sensorid", DataTypes.INT())
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
                        .topic("cctrx")
                        .startFromLatest()
                        .property("bootstrap.servers", brokerURI)
                        .property("group.id", use_case_id)
                        .property("INTERCEPTOR_CLASSES_CONFIG", "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringConsumerInterceptor")
        )
                .withFormat(new Json())
                .withSchema(schemaTableIoT)
                .createTemporaryTable("JSONSinkTable");


        String sql = "SELECT " +
                "  JSONSinkTable.cc_id" +
                ", JSONSinkTable.shop_id" +
                ", JSONSinkTable.shop_name" +
                ", JSONSinkTable.fx" +
                ", JSONSinkTable.fx_account" +
                ", JSONSinkTable.amount_orig" +
                ", lookupValues.city" +
                ", lookupValues.lat" +
                ", lookupValues.lon " +
                "FROM JSONSinkTable, lookupValues " +
                "WHERE JSONSinkTable.shop_id = lookupValues.sensorid";

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
                    + "," + "\"cc_id\"" + ":" + value.getField(0)
                    + "," + "\"shop_id\"" + ":" + value.getField(1)
                    + "," + "\"shop_name\"" + ":" + value.getField(2)
                    + "," + "\"fx\"" + ":" + value.getField(3)
                    + "," + "\"fx_account\"" + ":" + value.getField(4)
                    + "," + "\"amount_orig\"" + ":" + value.getField(5)
                    + "," + "\"city\"" + ":"+ "\"" + value.getField(6).toString().trim() + "\""
                    + "," + "\"lat\"" + ":" + value.getField(7)
                    + "," + "\"long\"" + ":" + value.getField(8) + "}";
            return str.getBytes();
        }

        @Override
        public String getTargetTopic(Row row) {
            return null;
        }

    }

}
