package consumer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class IoTJSONDeserializer implements FlatMapFunction<String, Tuple15<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {
    private transient ObjectMapper jsonParser;

    @Override
    public void flatMap(String value, Collector<Tuple15<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

        Long sensorts = jsonNode.get("sensor_ts").asLong();
        Integer sensorid = jsonNode.get("sensor_id").asInt();
        Integer sensor0 = jsonNode.get("sensor_0").asInt();
        Integer sensor1 = jsonNode.get("sensor_1").asInt();
        Integer sensor2 = jsonNode.get("sensor_2").asInt();
        Integer sensor3 = jsonNode.get("sensor_3").asInt();
        Integer sensor4 = jsonNode.get("sensor_4").asInt();
        Integer sensor5 = jsonNode.get("sensor_5").asInt();
        Integer sensor6 = jsonNode.get("sensor_6").asInt();
        Integer sensor7 = jsonNode.get("sensor_7").asInt();
        Integer sensor8 = jsonNode.get("sensor_8").asInt();
        Integer sensor9 = jsonNode.get("sensor_9").asInt();
        Integer sensor10 = jsonNode.get("sensor_10").asInt();
        Integer sensor11 = jsonNode.get("sensor_11").asInt();


        out.collect(new Tuple15<>(sensorts, sensorid, sensor0, sensor1,sensor2,sensor3,sensor4,sensor5,sensor6,sensor7,sensor8,sensor9,sensor10,sensor11, 1));
    }

}
