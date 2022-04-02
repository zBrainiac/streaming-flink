package consumer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class FSIJSONDeserializerCreditCardTrxTuple10 implements FlatMapFunction<String, Tuple10<Integer, String, String, Double, Integer, String, String, String, String, Integer>> {
    private transient ObjectMapper jsonParser;

    /**
     * orig msg:
     *  {"timestamp":1644133968404,"cc_id":"5178-7445-3092-4485","cc_type":"Diners Club","shop_id":3,"shop_name":"SihlCity","fx":"EUR","fx_account":"EUR","amount_orig":70.32}
     */
    @Override
    public void flatMap(String value, Collector<Tuple10<Integer, String, String, Double, Integer, String, String, String, String, Integer>> out) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

        // get all fields from JSONObject to tuple
        Integer trxtsd = jsonNode.get("timestamp").asInt();
        String ccid = jsonNode.get("cc_id").toString();
        String cctype = jsonNode.get("cc_type").toString();
        Double amountorig = jsonNode.get("amount_orig").asDouble();
        Integer shopid = jsonNode.get("shop_name").asInt();
        String shopname = jsonNode.get("shop_name").toString();
        String fx = jsonNode.get("fx").toString();
        String fxaccount = jsonNode.get("fx_account").toString();
        String fxfx = jsonNode.get("fx") + "_" + jsonNode.get("fx_account") ;
        out.collect(new Tuple10<>(trxtsd, ccid, cctype, amountorig, shopid, shopname, fx, fxaccount, fxfx, 1));

    }

}
