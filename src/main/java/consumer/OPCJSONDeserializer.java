package consumer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class OPCJSONDeserializer implements FlatMapFunction<String, Tuple4<String, String, Double, Integer>> {
    private transient ObjectMapper jsonParser;

    @Override
    public void flatMap(String value, Collector<Tuple4<String, String, Double, Integer>> out) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            /*
             get sensor_ts, sensor_id, sensor_0 AND sensor_1 from JSONObject
             {"val":"-0.813473","tagname":"Sinusoid","unit":"Hydrocracker","ts":"2020-03-13T11:17:53Z"}
            */

        String ts = jsonNode.get("ts").asText();
        String opc_tag = jsonNode.get("tagname").asText();
        Double opc_value = jsonNode.get("val").asDouble();
        out.collect(new Tuple4<>(ts, opc_tag, opc_value, 1));
    }

}
