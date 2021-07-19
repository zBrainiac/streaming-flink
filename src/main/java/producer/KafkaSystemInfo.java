package producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;


/**
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath streaming-flink-0.4.1.0.jar producer.KafkaSystemInfo localhost:9092
 *
 * @author Marcel Daeppen
 * @version 2020/12/28 12:14
 */

public class KafkaSystemInfo {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSystemInfo.class);
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


        try (Producer<String, byte[]> producer = createProducer()) {
            for (int i = 0; i < 1000000; i++) {
                publishMessage(producer);
                Thread.sleep(sleeptime);
            }
        }
    }

    private static Producer<String, byte[]> createProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-SystemInfo");
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");
        return new KafkaProducer<>(config);
    }

    private static void publishMessage(Producer<String, byte[]> producer) throws Exception {

        String key = UUID.randomUUID().toString();

        ObjectNode messageJsonObject = jsonObject();
        byte[] valueJson = objectMapper.writeValueAsBytes(messageJsonObject);

        ProducerRecord<String, byte[]> eventrecord = new ProducerRecord<>("SystemInfo", key, valueJson);

        RecordMetadata msg = producer.send(eventrecord).get();

        LOG.info(new StringBuilder().append("Published ").append(msg.topic()).append("/").append(msg.partition()).append("/").append(msg.offset()).append(" (key=").append(key).append(") : ").append(messageJsonObject).toString());

    }

    // build random json object
    private static ObjectNode jsonObject() {

        SystemInfo systemInfo = new SystemInfo();
        HardwareAbstractionLayer hardware = systemInfo.getHardware();
        CentralProcessor processor = hardware.getProcessor();
        GlobalMemory memory = hardware.getMemory();

        List<NetworkIF> networkIFs = hardware.getNetworkIFs();

        ObjectNode report = objectMapper.createObjectNode();
        report.put("sensor_ts", Instant.now().toEpochMilli());
        report.put("MAC_addr", networkIFs.get(2).getMacaddr());
        report.put("NET_Collisions ", networkIFs.get(2).getCollisions());

        report.put("NET_Recv_Bytes", networkIFs.get(2).getBytesRecv());
        report.put("NET_Send_Bytes", networkIFs.get(2).getBytesSent());
        long[] ticks = processor.getSystemCpuLoadTicks();
        report.put("CPU, IOWait, and IRQ ticks @ 1 sec:", Arrays.toString(ticks));

        // per core CPU
        double[] loadAverage = processor.getSystemLoadAverage(3);
        String Avgformat = " %.2f";
        report.put("CPU load averages:",
                (loadAverage[0] < 0 ? " N/A" : String.format(Avgformat, loadAverage[0]))
                        + (loadAverage[1] < 0 ? " N/A" : String.format(Avgformat, loadAverage[1]))
                        + (loadAverage[2] < 0 ? " N/A" : String.format(Avgformat, loadAverage[2])));


        report.put("CPU Context Switches", processor.getContextSwitches());
        report.put("MEM Total", memory.getTotal());
        report.put("MEM Available", memory.getAvailable());
        report.put("MEM Capacity", memory.getPhysicalMemory().get(0).getCapacity());
        report.put("MEM Page size", memory.getPageSize());


        return report;
    }

    public static void setsleeptime(long sleeptime) {
        KafkaSystemInfo.sleeptime = sleeptime;
    }
}