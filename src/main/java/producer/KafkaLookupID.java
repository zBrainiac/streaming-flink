package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Create Kafka Topic:
 * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 5 --topic kafka_lookupid &&
 * bin/kafka-topics.sh --list --bootstrap-server localhost:9092 &&
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka_lookupid
 * <p>
 * <p>
 * output:
 * Current time is 2020-08-30T15:45:24.485Z
 * <p>
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath streaming-flink-0.4.0.0.jar producer.KafkaLookupID localhost:9092
 *
 * @author Marcel Daeppen
 * @version 2021/07/04 17:14
 */

public class KafkaLookupID {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLookupID.class);
    private static final String LOGGERMSG = "Program prop set {}";

    private static String brokerURI = "localhost:9092";
    private static long sleeptime = 1000;


    public static void main(String[] args) throws Exception {

        if (args.length == 1) {
            brokerURI = args[0];
            String parm = "'use customized URI' = " + brokerURI + " & 'use default sleeptime' = " + sleeptime;
            LOG.info(LOGGERMSG, parm);
        } else if (args.length == 2) {
            brokerURI = args[0];
            setsleeptime(Long.parseLong(args[1]));
            String parm = "'use customized URI' = " + brokerURI + " & 'use customized sleeptime' = " + sleeptime;
            LOG.info(LOGGERMSG, parm);
        } else {
            String parm = "'use default URI' = " + brokerURI + " & 'use default sleeptime' = " + sleeptime;
            LOG.info(LOGGERMSG, parm);
        }

        //create kafka producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-Kafka-Simple");
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {

            ArrayList<String> al = new ArrayList<>();
            al.add("1,Geneva,46.195602,6.148113");
            al.add("2,Zürich,47.366667,8.55");
            al.add("3,Basel,47.558395,7.573271");
            al.add("4,Bern,46.916667,7.466667");
            al.add("5,Lausanne,46.533333,6.666667");
            al.add("6,Lucerne,47.083333,8.266667");
            al.add("7,Lugano,46.009279,8.955576");
            al.add("8,Sankt Fiden,47.43162,9.39845");
            al.add("9,Chur,46.856753,9.526918");
            al.add("10,Schaffhausen,47.697316,8.634929");
            al.add("11,Fribourg,46.79572,7.154748");
            al.add("12,Neuchâtel,46.993089,6.93005");
            al.add("13,Tripon,46.270839,7.317785");
            al.add("14,Zug,47.172421,8.517445");
            al.add("15,Frauenfeld,47.55993,8.8998");
            al.add("16,Bellinzona,46.194902,9.024729");
            al.add("17,Aarau,47.389616,8.052354");
            al.add("18,Herisau,47.38271,9.27186");
            al.add("19,Solothurn,47.206649,7.516605");
            al.add("20,Schwyz,47.027858,8.656112");
            al.add("21,Liestal,47.482779,7.742975");
            al.add("22,Delémont,47.366429,7.329005");
            al.add("23,Sarnen,46.898509,8.250681");
            al.add("24,Altdorf,46.880422,8.644409");
            al.add("25,Stansstad,46.97731,8.34005");
            al.add("26,Glarus,47.04057,9.068036");
            al.add("27,Appenzell,47.328414,9.409647");
            al.add("28,Saignelégier,47.255435,6.994608");
            al.add("29,Affoltern am Albis,47.281224,8.45346");
            al.add("30,Cully,46.488301,6.730109");
            al.add("31,Romont,46.696483,6.918037");
            al.add("32,Aarberg,47.043835,7.27357");
            al.add("33,Scuol,46.796756,10.305946");
            al.add("34,Fleurier,46.903265,6.582135");
            al.add("35,Unterkulm,47.30998,8.11371");
            al.add("36,Stans,46.95805,8.36609");
            al.add("37,Lichtensteig,47.337551,9.084078");
            al.add("38,Yverdon-les-Bains,46.777908,6.635502");
            al.add("39,Boudry,46.953019,6.83897");
            al.add("40,Balsthal,47.31591,7.693047");
            al.add("41,Dornach,47.478042,7.616417");
            al.add("42,Lachen,47.19927,8.85432");
            al.add("43,Payerne,46.82201,6.93608");
            al.add("44,Baden,47.478029,8.302764");
            al.add("45,Bad Zurzach,47.589169,8.289621");
            al.add("46,Tafers,46.814829,7.218519");
            al.add("47,Haslen,47.369308,9.367519");
            al.add("48,Echallens,46.642498,6.637324");
            al.add("49,Rapperswil-Jona,47.228942,8.833889");
            al.add("50,Bulle,46.619499,7.056743");
            al.add("51,Bülach,47.518898,8.536967");
            al.add("52,Sankt Gallen,47.43639,9.388615");
            al.add("53,Wil,47.460507,9.04389");
            al.add("54,Zofingen,47.289945,7.947274");
            al.add("55,Vevey,46.465264,6.841168");
            al.add("56,Renens,46.539894,6.588096");
            al.add("57,Brugg,47.481527,8.203014");
            al.add("58,Laufenburg,47.559248,8.060446");
            al.add("59,La Chaux-de-Fonds,47.104417,6.828892");
            al.add("60,Andelfingen,47.594829,8.679678");
            al.add("61,Dietikon,47.404446,8.394984");
            al.add("62,Winterthur,47.50564,8.72413");
            al.add("63,Thun,46.751176,7.621663");
            al.add("64,Le Locle,47.059533,6.752278");
            al.add("65,Bremgarten,47.352604,8.329955");
            al.add("66,Tiefencastel,46.660138,9.57883");
            al.add("67,Saint-Maurice,46.218257,7.003196");
            al.add("68,Cernier,47.057356,6.894757");
            al.add("69,Ostermundigen,46.956112,7.487187");
            al.add("70,Estavayer-le-Lac,46.849125,6.845805");
            al.add("71,Frutigen,46.58782,7.64751");
            al.add("72,Muri,47.270428,8.3382");
            al.add("73,Murten,46.92684,7.110343");
            al.add("74,Rheinfelden,47.553587,7.793839");
            al.add("75,Gersau,46.994189,8.524996");
            al.add("76,Schüpfheim,46.951613,8.017235");
            al.add("77,Saanen,46.489557,7.259609");
            al.add("78,Olten,47.357058,7.909101");
            al.add("79,Domat/Ems,46.834827,9.450752");
            al.add("80,Münchwilen,47.47788,8.99569");
            al.add("81,Horgen,47.255924,8.598672");
            al.add("82,Willisau,47.119362,7.991459");
            al.add("83,Rorschach,47.477166,9.485434");
            al.add("84,Morges,46.511255,6.495693");
            al.add("85,Interlaken,46.683872,7.866376");
            al.add("86,Sursee,47.170881,8.111132");
            al.add("87,Küssnacht,47.085571,8.442057");
            al.add("88,Weinfelden,47.56571,9.10701");
            al.add("89,Pfäffikon,47.365728,8.78595");
            al.add("90,Meilen,47.270429,8.643675");
            al.add("91,Langnau,46.93936,7.78738");
            al.add("92,Kreuzlingen,47.650512,9.175038");
            al.add("93,Nidau,47.129167,7.238464");
            al.add("94,Igis,46.945308,9.57218");
            al.add("95,Ilanz,46.773071,9.204486");
            al.add("96,Einsiedeln,47.12802,8.74319");
            al.add("97,Wangen,47.231995,7.654479");
            al.add("98,Hinwil,47.29702,8.84348");
            al.add("99,Hochdorf,47.168408,8.291788");
            al.add("100,Thusis,46.697524,9.440202");
            al.add("101,Lenzburg,47.384048,8.181798");
            al.add("102,Dielsdorf,47.480247,8.45628");
            al.add("103,Mörel-Filet,46.355548,8.044112");
            al.add("104,Münster-Geschinen,46.491704,8.272063");
            al.add("105,Martigny,46.101915,7.073989");
            al.add("106,Brig-Glis,46.3145,7.985796");
            al.add("107,Davos,46.797752,9.82702");
            al.add("108,Uster,47.352097,8.716687");
            al.add("109,Altstätten,47.376433,9.554989");
            al.add("110,Courtelary,47.179369,7.072954");
            al.add("111,Porrentruy,47.415327,7.075221");

            for (int i = 0; i < 111; i++) {
                String recordValue = al.get(i); //"Current time is " + Instant.now().toString();

                ProducerRecord<String, String> eventrecord = new ProducerRecord<>("kafka_lookupid", recordValue);

                //produce the eventrecord
                RecordMetadata md = producer.send(eventrecord).get();

                LOG.info(new StringBuilder().append("Published: ").append("topic=").append(md.topic()).append(", ").append("partition=").append(md.partition()).append(", ").append("offset=").append(md.offset()).append(", ").append("timestamp=").append(md.timestamp()).append(", ").append("payload=").append(recordValue).toString());

                producer.flush();
            }
        }
    }

    public static void setsleeptime(long sleeptime) {
        KafkaLookupID.sleeptime = sleeptime;
    }

}