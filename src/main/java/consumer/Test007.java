package consumer;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

/**
 * SELECT gk, COUNT(*) AS pv
 * FROM tab
 * GROUP BY HOP(rowtime, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE), gk
 */
public class Test007 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);


        TableSource<?> tableSource = CsvTableSource
                .builder()
                .path("data/lookup.csv")
                .field("sensorid", Types.LONG)
                .field("location", Types.STRING)
                .fieldDelimiter(",")
                .lineDelimiter("\n")
                .ignoreFirstLine()
                .ignoreParseErrors()
                .build();
        bsTableEnv.registerTableSource("lookupValues", tableSource);

        System.out.println("\n CSV Lookup Table Created with Schema : \n");

        //Create a Table Object with the product_sales table.
        Table lookupValuesTable = bsTableEnv
                .scan("lookupValues");

        lookupValuesTable.printSchema();

        Table table = bsTableEnv.scan("lookupValues");

        DataStream<Row> stream = bsTableEnv.toAppendStream(table, Row.class);
        stream.print("csv print: ");

        bsTableEnv.execute("Table SQL Query");
    }

}