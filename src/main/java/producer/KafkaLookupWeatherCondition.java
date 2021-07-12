package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
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
 * java -classpath streaming-flink-0.4.1.0.jar producer.KafkaLookupWeatherCondition localhost:9092
 *
 * @author Marcel Daeppen
 * @version 2021/07/04 17:14
 */

public class KafkaLookupWeatherCondition {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLookupWeatherCondition.class);
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
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-Kafka-LookupWeatherCondition");
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");


        ArrayList<String> al = new ArrayList<>();
            al.add("1," + Instant.now().toEpochMilli() + ",19.6,0,0,413,76.9,15.4,198,9.7,16.9,950.5,1011.5,1013.7,,,,,,,,");
            al.add("2," + Instant.now().toEpochMilli() + ",18.6,0,0,155,77.4,14.6,138,2.9,7.6,946.6,1011.9,1014,,,,,,,,");
            al.add("3," + Instant.now().toEpochMilli() + ",13,0.1,0,251,88.7,11.2,63,11.2,18,866.7,,1016.6,1489.2,,,,,,,");
            al.add("4," + Instant.now().toEpochMilli() + ",17.8,0,0,147,87.9,15.8,164,7.2,11.9,968.8,1012.9,1013.9,,,,,,,,");
            al.add("5," + Instant.now().toEpochMilli() + ",18.6,0,0,312,80.9,15.3,317,4,7.6,962,1012.3,1013.7,,,,,,,,");
            al.add("6," + Instant.now().toEpochMilli() + ",19.9,0,0,378,87.4,17.7,356,5.4,9.7,966.5,1012,1013.5,,,,,,,,");
            al.add("7," + Instant.now().toEpochMilli() + ",17.4,0,0,110,72.6,12.4,273,1.4,2.9,901.5,,1014.9,1488.2,,,,,,,");
            al.add("8," + Instant.now().toEpochMilli() + ",12.8,0,0,203,87.1,10.7,39,11.2,18.7,855.3,,1017.1,1491.1,,,,,,,");
            al.add("9," + Instant.now().toEpochMilli() + ",11.7,0,0,199,91,10.3,52,3.2,7.6,811.1,,1018.2,1484.8,,,,,,,");
            al.add("10," + Instant.now().toEpochMilli() + ",20,0,0,322,72.3,14.9,294,15.1,23.8,955.1,1011.5,1013.4,,,,,,,,");
            al.add("11," + Instant.now().toEpochMilli() + ",,,0,440,,,,,,,,,,,230,18.7,22.7,13.8,85.5,11.4");
            al.add("12," + Instant.now().toEpochMilli() + ",22,0,4,708,70.2,16.3,201,7.2,11.5,975.2,1011.4,1012.8,,,,,,,,");
            al.add("13," + Instant.now().toEpochMilli() + ",15.2,0,,,74.8,10.8,312,3.6,6.8,,,,,,,,,,,");
            al.add("14," + Instant.now().toEpochMilli() + ",18.8,0,0,430,80.8,15.4,241,6.8,11.5,949.7,1012.6,1014.6,,,,,,,,");
            al.add("15," + Instant.now().toEpochMilli() + ",20.8,0,0,413,74.8,16.2,198,6.8,12.2,974.3,1011.7,1013,,,,,,,,");
            al.add("16," + Instant.now().toEpochMilli() + ",20.1,0,0,168,72.2,14.9,56,1.4,3.6,979.8,1011.9,1012.8,,,,,,,,");
            al.add("17," + Instant.now().toEpochMilli() + ",13.6,0,,,83.8,10.9,79,3.6,6.5,,,,,,,,,,,");
            al.add("18," + Instant.now().toEpochMilli() + ",19.8,0,0,470,77.2,15.7,279,9,14.8,954,1011.6,1013.6,,,,,,,,");
            al.add("19," + Instant.now().toEpochMilli() + ",13.4,0,,,79.5,9.9,278,9.7,15.1,,,,,,,,,,,");
            al.add("20," + Instant.now().toEpochMilli() + ",17.2,0,0,300,87.9,15.2,220,11.5,17.3,935.4,,1015.1,1494.6,,,,,,,");
            al.add("21," + Instant.now().toEpochMilli() + ",12.7,0,0,280,85.1,10.3,342,2.2,5,844.9,,1017.2,1488.3,,,,,,,");
            al.add("22," + Instant.now().toEpochMilli() + ",15.8,0,0,199,89.8,14.1,62,6.1,9.4,920.5,,1015.5,1492.7,,,,,,,");
            al.add("23," + Instant.now().toEpochMilli() + ",17.8,,,,85.3,15.3,124,7.9,13,969.8,1013,1014.1,,,,,,,,");
            al.add("24," + Instant.now().toEpochMilli() + ",,0,,,,,263,2.9,5.4,947.6,,1014,,,,,,,,");
            al.add("25," + Instant.now().toEpochMilli() + ",20.2,0,0,442,75.2,15.7,211,5,10.1,967.7,1012,1013.5,,,,,,,,");
            al.add("26," + Instant.now().toEpochMilli() + ",14.3,0,0,109,61,6.9,115,10.1,17.6,802,,1018.1,1475.3,,,,,,,");
            al.add("27," + Instant.now().toEpochMilli() + ",13.4,0,0,269,94.9,12.6,190,6.8,12.2,879.3,,1016.5,1491.2,,,,,,,");
            al.add("28," + Instant.now().toEpochMilli() + ",19.3,0,0,214,79.9,15.7,134,1.4,3.6,963.9,1012.1,1013.6,,,,,,,,");
            al.add("29," + Instant.now().toEpochMilli() + ",19.7,0,0,415,78.4,15.8,151,11.2,17.3,961.5,1012,1013.7,,,,,,,,");
            al.add("30," + Instant.now().toEpochMilli() + ",10.5,0,0,370,94.4,9.6,218,13.7,22.3,839,,1017.1,1486.9,,,,,,,");
            al.add("31," + Instant.now().toEpochMilli() + ",14.6,0,0,396,86.1,12.3,250,6.8,15.1,886.5,,1016.1,1491.2,,,,,,,");
            al.add("32," + Instant.now().toEpochMilli() + ",21,0,0,260,68.4,15,26,13.7,20.5,948.1,1010.7,1013.3,,,,,,,,");
            al.add("33," + Instant.now().toEpochMilli() + ",14.5,0,0,233,91.7,13.2,227,1.8,3.6,898,,1015.9,1491,,,,,,,");
            al.add("34," + Instant.now().toEpochMilli() + ",13.2,0,0,221,82.4,10.3,79,7.9,11.9,832.9,,1018,1490.8,,,,,,,");
            al.add("35," + Instant.now().toEpochMilli() + ",12.8,0.1,,,94.4,11.9,218,7.2,12.2,,,,,,,,,,,");
            al.add("36," + Instant.now().toEpochMilli() + ",8.7,0,0,307,54.7,0.1,279,29.5,43.6,753.7,,1020.6,,3095.8,,,,,,");
            al.add("37," + Instant.now().toEpochMilli() + ",18.3,0,,,80,14.8,20,4,8.6,,,,,,,,,,,");
            al.add("38," + Instant.now().toEpochMilli() + ",8,,0,183,86.9,6,209,6.8,11.9,753.9,,1020.1,,3090.2,,,,,,");
            al.add("39," + Instant.now().toEpochMilli() + ",19.3,0,0,314,83.1,16.4,208,8.6,13.3,963.2,1012.4,1014,,,,,,,,");
            al.add("40," + Instant.now().toEpochMilli() + ",14,0,0,258,77.3,10.1,26,16.2,20.5,838.9,,1017,1484.5,,,,,,,");
            al.add("41," + Instant.now().toEpochMilli() + ",20.3,0,0,402,76.2,16,333,1.8,4.3,961.5,1011.5,1013.2,,,,,,,,");
            al.add("42," + Instant.now().toEpochMilli() + ",15.5,0,0,86,78.8,11.8,52,1.8,4.7,879.5,,1015.7,1487.1,,,,,,,");
            al.add("43," + Instant.now().toEpochMilli() + ",22.8,0,0,368,61.8,15.1,247,4.3,11.5,941.5,,1014.3,1506.3,,,,,,,");
            al.add("44," + Instant.now().toEpochMilli() + ",4.3,,0,439,87.9,2.5,239,13.3,23.4,717.3,,1022.1,,3091.7,,,,,,");
            al.add("45," + Instant.now().toEpochMilli() + ",19,0,0,257,77.4,15,266,7.9,16.9,952.8,1012.2,1014.1,,,,,,,,");
            al.add("46," + Instant.now().toEpochMilli() + ",17.2,0,0,361,78.7,13.5,288,9,13.7,910.2,,1015.2,1492.2,,,,,,,");
            al.add("47," + Instant.now().toEpochMilli() + ",16.2,0,0,175,85.3,13.7,319,4,7.6,905.2,,1015.4,1490.7,,,,,,,");
            al.add("48," + Instant.now().toEpochMilli() + ",15.8,0,0,237,81.2,12.6,214,5,10.8,896.9,,1015.7,1491,,,,,,,");
            al.add("49," + Instant.now().toEpochMilli() + ",17.1,,0,127,87.6,15,3,8.6,15.5,957.5,1012.9,1014.2,,,,,,,,");
            al.add("50," + Instant.now().toEpochMilli() + ",11.5,0,0,377,88.1,9.6,274,5,10.1,816.2,,1018.3,1488,,,,,,,");
            al.add("51," + Instant.now().toEpochMilli() + ",21.2,0,10,1017,62.7,13.8,235,10.4,17.3,944.1,1011.1,1013.9,,,,,,,,");
            al.add("52," + Instant.now().toEpochMilli() + ",15.9,0,,,91.6,14.5,284,20.2,25.6,,,,,,,,,,,");
            al.add("53," + Instant.now().toEpochMilli() + ",18.1,0,0,300,87.2,15.9,247,4,10.1,938.7,,1014.6,1493.8,,,,,,,");
            al.add("54," + Instant.now().toEpochMilli() + ",15.8,0,0,193,89.6,14.1,327,4.3,7.6,,,,,,,,,,,");
            al.add("55," + Instant.now().toEpochMilli() + ",18.1,0,0,413,84.3,15.4,294,4,9.4,965.5,1012.9,1014.1,,,,,,,,");
            al.add("56," + Instant.now().toEpochMilli() + ",18.1,0,,,83,15.2,103,4.7,9.7,,,,,,,,,,,");
            al.add("57," + Instant.now().toEpochMilli() + ",18.8,0,0,287,83.8,16,101,2.2,4,958.5,1012.3,1013.9,,,,,,,,");
            al.add("58," + Instant.now().toEpochMilli() + ",19,0,0,183,77.4,15,303,14.4,23.4,953.1,1011.9,1013.8,,,,,,,,");
            al.add("59," + Instant.now().toEpochMilli() + ",2.6,,0,340,77.2,1,120,9,18.4,696.9,,1023.4,,3095.5,,,,,,");
            al.add("60," + Instant.now().toEpochMilli() + ",18.9,0,0,243,81.7,15.7,260,6.1,9.7,963.4,1012.5,1014,,,,,,,,");
            al.add("61," + Instant.now().toEpochMilli() + ",9.5,0,0,268,82.7,6.7,357,9,13,803.5,,1019,1491.7,,,,,,,");
            al.add("62," + Instant.now().toEpochMilli() + ",20.6,0,0,161,74.8,16,145,4,6.1,974.8,1011.9,1013.2,,,,,,,,");
            al.add("63," + Instant.now().toEpochMilli() + ",12,0,0,472,90.8,10.5,,,,838.2,,1017.4,1488.5,,,,,,,");
            al.add("64," + Instant.now().toEpochMilli() + ",15.4,0,,,88.7,13.5,10,2.9,6.5,906,,1015.4,1489.2,,,,,,,");
            al.add("65," + Instant.now().toEpochMilli() + ",21,0,3,672,72.2,15.8,230,8.3,17.6,968.3,1011.8,1013.3,,,,,,,,");
            al.add("66," + Instant.now().toEpochMilli() + ",8.4,0,0,153,87.2,6.4,183,13,18,772.2,,1019.8,1486,,,,,,,");
            al.add("67," + Instant.now().toEpochMilli() + ",20.5,0,0,400,77.9,16.5,278,11.2,16.9,961.5,1011.6,1013.4,,,,,,,,");
            al.add("68," + Instant.now().toEpochMilli() + ",19.7,0,0,379,79.5,16.1,212,7.2,13,964.1,1012,1013.5,,,,,,,,");
            al.add("69," + Instant.now().toEpochMilli() + ",14.6,0,0,469,89.1,12.8,241,10.4,15.8,886.7,,1016,1490.2,,,,,,,");
            al.add("70," + Instant.now().toEpochMilli() + ",19.6,0,0,191,72.8,14.6,,,,932.6,,1013.8,1490.7,,,,,,,");
            al.add("71," + Instant.now().toEpochMilli() + ",17.7,0,0,249,86.1,15.3,63,5.4,10.4,946.6,1012.4,1014.3,,,,,,,,");
            al.add("72," + Instant.now().toEpochMilli() + ",0.8,,0,462,96.8,1.2,134,23.4,31.7,658.8,,1024.1,,3088.2,,,,,,");
            al.add("73," + Instant.now().toEpochMilli() + ",19.5,0,0,374,78.7,15.7,209,10.4,15.5,957.3,1012.6,1014.4,,,,,,,,");
            al.add("74," + Instant.now().toEpochMilli() + ",17.4,0,10,754,68.6,11.6,179,11.5,19.1,895.2,,1015.4,1491.5,,,,,,,");
            al.add("75," + Instant.now().toEpochMilli() + ",16.5,0,0,267,74.6,12,185,11.2,16.9,898.5,,1015.2,1488.7,,,,,,,");
            al.add("76," + Instant.now().toEpochMilli() + ",10.1,0,0,454,88.3,8.3,179,18.4,33.8,832.1,,1018.1,1493.4,,,,,,,");
            al.add("77," + Instant.now().toEpochMilli() + ",19,0,,,81.5,15.8,,,,,,,,,,,,,,");
            al.add("78," + Instant.now().toEpochMilli() + ",17.7,0,0,416,85.8,15.3,277,5.4,8.6,928.5,,1014.9,1494,,,,,,,");
            al.add("79," + Instant.now().toEpochMilli() + ",8.6,0,0,427,85.9,6.4,224,18,37.1,801.8,,1018.7,1489.7,,,,,,,");
            al.add("80," + Instant.now().toEpochMilli() + ",20.5,0,0,446,78.4,16.6,116,1.4,5.4,972.5,1011.7,1013,,,,,,,,");
            al.add("81," + Instant.now().toEpochMilli() + ",3.8,,0,237,91.5,2.5,226,10.8,21.2,731.3,,1021.5,,3089.6,,,,,,");
            al.add("82," + Instant.now().toEpochMilli() + ",16.5,0,,,76.3,12.3,142,2.9,6.8,895.9,,1015.6,1491.7,,,,,,,");
            al.add("83," + Instant.now().toEpochMilli() + ",1.3,,0,344,90.2,0.1,216,27.7,46.1,710.5,,1021.7,,3084.7,,,,,,");
            al.add("84," + Instant.now().toEpochMilli() + ",15,0.2,,,89.4,13.3,184,3.6,7.9,,,,,,,,,,,");
            al.add("85," + Instant.now().toEpochMilli() + ",20.4,0,0,218,69.2,14.6,231,2.2,4.7,969.1,1011.8,1013.3,,,,,,,,");
            al.add("86," + Instant.now().toEpochMilli() + ",22.2,0,0,244,73.1,17.2,81,0.7,2.2,977.5,1011.8,1013.2,,,,,,,,");
            al.add("87," + Instant.now().toEpochMilli() + ",18.7,0,0,117,83.4,15.8,181,7.2,11.2,960.3,1012.4,1013.9,,,,,,,,");
            al.add("88," + Instant.now().toEpochMilli() + ",15.4,,0,336,90.8,13.9,200,13.3,16.9,917.1,,1014.7,1485.4,,,,,,,");
            al.add("89," + Instant.now().toEpochMilli() + ",21.1,0,0,146,73.7,16.2,331,0.7,1.4,988.3,1011.9,1012.6,,,,,,,,");
            al.add("90," + Instant.now().toEpochMilli() + ",17.1,0,,,87.8,15.1,172,3.2,7.2,,,,,,,,,,,");
            al.add("91," + Instant.now().toEpochMilli() + ",18.9,0,0,304,85.3,16.4,182,17.3,23.4,962.7,1012.6,1014.1,,,,,,,,");
            al.add("92," + Instant.now().toEpochMilli() + ",8.7,,0,170,81.5,5.7,143,21.6,27.7,781.8,,1020,1491.9,,,,,,,");
            al.add("93," + Instant.now().toEpochMilli() + ",17.5,0,0,261,79.8,14,299,5,7.6,945.6,1012.6,1014.5,,,,,,,,");
            al.add("94," + Instant.now().toEpochMilli() + ",16.8,0,,,73.6,12.1,312,4,6.1,917.9,,1014.9,1490.4,,,,,,,");
            al.add("95," + Instant.now().toEpochMilli() + ",14,0,0,509,82.7,11.1,250,6.5,9.7,856.7,,1017,1490.5,,,,,,,");
            al.add("96," + Instant.now().toEpochMilli() + ",13.5,0,0,367,97.2,13.1,155,3.6,9,839.5,,1018.4,1496.4,,,,,,,");
            al.add("97," + Instant.now().toEpochMilli() + ",5.2,,0,479,58.9,2.2,47,10.8,18.4,717.9,,1022.1,,3093,,,,,,");
            al.add("98," + Instant.now().toEpochMilli() + ",19.6,0,0,381,74.5,14.9,218,5.4,9.7,960.4,1012.1,1013.8,,,,,,,,");
            al.add("99," + Instant.now().toEpochMilli() + ",13.7,0,0,522,73.9,9.1,346,15.5,25.2,840.6,,1017.2,1487,,,,,,,");
            al.add("100," + Instant.now().toEpochMilli() + ",20.6,0,10,721,73.3,15.7,285,1.4,4.7,972.6,1011.9,1013.2,,,,,,,,");
            al.add("101," + Instant.now().toEpochMilli() + ",18.6,0,0,278,82.4,15.5,315,3.6,6.8,957.6,1012.5,1014.1,,,,,,,,");
            al.add("102," + Instant.now().toEpochMilli() + ",,,0,340,,,,,,,,,,,236,8.6,13.7,15.6,93.1,14.5");
            al.add("103," + Instant.now().toEpochMilli() + ",10.8,,0,461,77.6,7,207,6.1,10.1,763.4,,1020,,3098.9,,,,,,");
            al.add("104," + Instant.now().toEpochMilli() + ",11.9,0,0,260,96.2,11.3,275,7.2,17.6,858.4,,1016.7,1487.7,,,,,,,");
            al.add("105," + Instant.now().toEpochMilli() + ",19,0,0,371,80.3,15.5,225,8.6,15.5,957.1,1012.4,1014.1,,,,,,,,");
            al.add("106," + Instant.now().toEpochMilli() + ",18.3,0,0,356,82.6,15.3,240,5,9.4,960.3,1012.9,1014.4,,,,,,,,");
            al.add("107," + Instant.now().toEpochMilli() + ",20.6,0,,,76.3,16.3,28,13.7,21.2,965.1,1011.7,1013.4,,,,,,,,");
            al.add("108," + Instant.now().toEpochMilli() + ",,,,,,,297,8.3,11.9,,,,,,,,,,,");
            al.add("109," + Instant.now().toEpochMilli() + ",15.7,0,0,318,94.5,14.8,176,3.2,6.1,919.4,,1015.3,1490.8,,,,,,,");
            al.add("110," + Instant.now().toEpochMilli() + ",9.8,0,0,175,79.3,6.4,148,28.8,37.4,775.3,,1020.4,1488.9,,,,,,,");
            al.add("111," + Instant.now().toEpochMilli() + ",18.7,0,0,316,78.3,14.8,220,8.6,13.3,956.4,1012.3,1014,,,,,,,,");
            al.add("112," + Instant.now().toEpochMilli() + ",7.9,0.1,0,89,100,7.9,212,6.8,25.9,789.3,,1018.8,1487.5,,,,,,,");
            al.add("113," + Instant.now().toEpochMilli() + ",16.1,0,0,109,75.3,11.7,349,1.8,6.1,901.7,,1015.4,1490.4,,,,,,,");
            al.add("114," + Instant.now().toEpochMilli() + ",2.8,0,0,257,80.8,0.2,151,9.4,23,682.5,,1024.3,,3096.8,,,,,,");
            al.add("115," + Instant.now().toEpochMilli() + ",7.1,0,0,218,74.3,2.8,195,29.9,42.5,737,,1021.2,,3093.7,,,,,,");
            al.add("116," + Instant.now().toEpochMilli() + ",15.5,0,0,302,87,13.3,283,7.9,13.7,896.3,,1015.8,1491.7,,,,,,,");
            al.add("117," + Instant.now().toEpochMilli() + ",18,0,0,195,69.5,12.3,245,13.3,21.2,892.1,,1015.5,1491.7,,,,,,,");
            al.add("118," + Instant.now().toEpochMilli() + ",18,0,0,227,88.3,16,154,2.9,6.5,960.4,1012.7,1014.1,,,,,,,,");
            al.add("119," + Instant.now().toEpochMilli() + ",,,,,,,89,4.7,8.3,963.7,,1013.2,,,,,,,,");
            al.add("120," + Instant.now().toEpochMilli() + ",9.8,0.1,0,87,85,7.4,236,1.4,3.6,810,,1019,1493.6,,,,,,,");
            al.add("121," + Instant.now().toEpochMilli() + ",18.4,0,0,488,75.8,14.1,165,7.2,11.9,942.5,,1014,1490,,,,,,,");
            al.add("122," + Instant.now().toEpochMilli() + ",13,0,0,186,77.1,9.1,121,3.6,6.5,835.4,,1018.3,1494,,,,,,,");
            al.add("123," + Instant.now().toEpochMilli() + ",17.4,0,0,448,92.9,16.2,240,11.9,18.4,930.9,,1014.5,1489.7,,,,,,,");
            al.add("124," + Instant.now().toEpochMilli() + ",14.2,0,0,265,64.8,7.7,237,29.9,43.9,827.3,,1017.1,1480.6,,,,,,,");
            al.add("125," + Instant.now().toEpochMilli() + ",17,0,,,88.5,15.1,238,5.8,9.7,923.2,,1014.8,1490.4,,,,,,,");
            al.add("126," + Instant.now().toEpochMilli() + ",20,0,0,315,76.5,15.7,254,10.1,15.8,961.5,1011.5,1013.2,,,,,,,,");
            al.add("127," + Instant.now().toEpochMilli() + ",19.6,0,,,77.5,15.6,307,14,26.3,,,,,,,,,,,");
            al.add("128," + Instant.now().toEpochMilli() + ",,,,,,,215,7.6,10.1,965.7,,1013.8,,,,,,,,");
            al.add("129," + Instant.now().toEpochMilli() + ",17.2,0,0,423,88.8,15.3,178,5.4,7.9,928.3,,1014.8,1491.5,,,,,,,");
            al.add("130," + Instant.now().toEpochMilli() + ",20.1,0,0,368,47.3,8.5,78,2.5,5.8,867.3,,1014.8,1478.9,,,,,,,");
            al.add("131," + Instant.now().toEpochMilli() + ",12.6,0,0,282,72.8,7.9,226,19.8,28.1,818.5,,1018.1,1486.4,,,,,,,");
            al.add("132," + Instant.now().toEpochMilli() + ",13.4,0,0,179,81.3,10.3,87,3.6,9.7,851.8,,1017.2,1490.9,,,,,,,");
            al.add("133," + Instant.now().toEpochMilli() + ",18.7,0,0,305,78.6,14.9,235,4.3,9.7,957.4,1012.5,1014.1,,,,,,,,");
            al.add("134," + Instant.now().toEpochMilli() + ",,,,,,,230,9.7,15.8,,,,,,,,,,,");
            al.add("135," + Instant.now().toEpochMilli() + ",,,5,814,,,,,,,,,,,176,7.9,15.1,17.4,83.9,14.7");
            al.add("136," + Instant.now().toEpochMilli() + ",17.5,0,0,191,83.3,14.6,289,4,9.4,924.6,,1014.6,1490.5,,,,,,,");
            al.add("137," + Instant.now().toEpochMilli() + ",17.2,0,0,311,61.8,9.8,46,6.8,12.6,859.7,,1015.9,1483.9,,,,,,,");
            al.add("138," + Instant.now().toEpochMilli() + ",22.6,0,0,263,73.3,17.6,214,5,9,971.7,1011.7,1013.4,,,,,,,,");
            al.add("139," + Instant.now().toEpochMilli() + ",,,,,,,260,11.9,15.8,966.2,,1013.1,,,,,,,,");
            al.add("140," + Instant.now().toEpochMilli() + ",5.4,0,0,177,100,5.4,261,16.9,27.7,752.8,,1020.2,,3085.4,,,,,,");
            al.add("141," + Instant.now().toEpochMilli() + ",17.6,0,0,248,87,15.4,107,3.2,6.5,947.8,1012.8,1014.6,,,,,,,,");
            al.add("142," + Instant.now().toEpochMilli() + ",2.2,,0,343,96.9,1.8,177,14,18,699.2,,1022.5,,3088.6,,,,,,");
            al.add("143," + Instant.now().toEpochMilli() + ",,,0,357,,,,,,,,,,,237,15.1,20.2,14.4,95,13.6");
            al.add("144," + Instant.now().toEpochMilli() + ",14.5,0,0,187,75.9,10.3,262,3.6,6.1,864.4,,1016.5,1489.2,,,,,,,");
            al.add("145," + Instant.now().toEpochMilli() + ",20.5,0,0,347,72.6,15.4,312,5.8,9,959.4,1011.4,1013.3,,,,,,,,");
            al.add("146," + Instant.now().toEpochMilli() + ",12.9,0,0,217,87.6,10.9,357,9,12.2,841.8,,1017.2,1487.7,,,,,,,");
            al.add("147," + Instant.now().toEpochMilli() + ",14.5,0,,,88.9,12.7,48,2.5,4,,,,,,,,,,,");
            al.add("148," + Instant.now().toEpochMilli() + ",18,0,0,168,84.8,15.4,136,5.4,11.2,,,,,,,,,,,");
            al.add("149," + Instant.now().toEpochMilli() + ",17,0,0,206,68.4,11.1,236,1.8,6.1,891.3,,1015.8,1493,,,,,,,");
            al.add("150," + Instant.now().toEpochMilli() + ",15.8,0,,,86.5,13.6,212,8.3,13.3,,,,,,,,,,,");
            al.add("151," + Instant.now().toEpochMilli() + ",18,0,0,363,76.9,13.9,91,5.4,9.4,939.4,,1014.1,1489.2,,,,,,,");
            al.add("152," + Instant.now().toEpochMilli() + ",5.4,0,0,169,97.6,5.1,148,5.8,12.6,736.9,,1021.4,,3092.1,,,,,,");
            al.add("153," + Instant.now().toEpochMilli() + ",19.9,0,2,616,76.5,15.6,236,9,17.3,963.8,1012.1,1013.7,,,,,,,,");
            al.add("154," + Instant.now().toEpochMilli() + ",19.9,0,0,335,72.9,14.9,270,4.7,7.9,956.6,1011.8,1013.7,,,,,,,,");
            al.add("155," + Instant.now().toEpochMilli() + ",20.6,0,0,463,74.4,15.9,,,,973.6,1012,1013.3,,,193,15.1,22.7,19.3,76.5,");
            al.add("156," + Instant.now().toEpochMilli() + ",13.5,0,0,339,72.8,8.7,53,7.2,11.9,834.7,,1017.4,1486.4,,,,,,,");
            al.add("157," + Instant.now().toEpochMilli() + ",20.1,0,0,365,74.9,15.5,283,9.4,16.6,961,1011.6,1013.3,,,,,,,,");
            al.add("158," + Instant.now().toEpochMilli() + ",18.9,0,0,343,78,15,302,3.6,7.9,948.4,1011.5,1013.6,,,,,,17.1,85,14.6");
            al.add("159," + Instant.now().toEpochMilli() + ",19.9,0,0,346,75.6,15.5,280,7.2,11.9,963,1011.7,1013.3,,,,,,,,");


        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {

            for (int il = 1; il <= 1000000; ++il) {


                for (int i = 0; i < 159; i++) {
                    String recordValue = al.get(i);

                    ProducerRecord<String, String> eventrecord = new ProducerRecord<>("kafka_LookupWeatherCondition", recordValue);

                    //produce the eventrecord
                    RecordMetadata md = producer.send(eventrecord).get();

                    LOG.info(new StringBuilder().append("Published: ").append("topic=").append(md.topic()).append(", ").append("partition=").append(md.partition()).append(", ").append("offset=").append(md.offset()).append(", ").append("timestamp=").append(md.timestamp()).append(", ").append("payload=").append(recordValue).toString());

                    producer.flush();
                }
                Thread.sleep(sleeptime);

            }
        }
    }

    public static void setsleeptime(long sleeptime) {
        KafkaLookupWeatherCondition.sleeptime = sleeptime;
    }

}