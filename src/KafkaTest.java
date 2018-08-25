import java.util.ArrayList;
import java.util.List;

public class KafkaTest {
    public static void main(String[] args) {
        String hosts = "192.168.100.70:9092,192.168.100.71:9092,192.168.100.72:9092";
        String in_topic = "tp.test.io";
        String out_topic = "tp.test.io";
        String groupid = "gp.test.java";

        ServerKafka kafka = new ServerKafka(hosts);
        kafka.get_market("market", "tp.java.test1");
        long start = System.currentTimeMillis();
        for (int i=0;i<100;i++) {
            String msg = kafka.get_new("au1812");
            System.out.println(String.format("第%d次获取：%s", i, msg));
        }
        long end = System.currentTimeMillis();
        kafka.stop();
        System.out.println(String.format("信息100条，耗时：%d",end-start));
    }
}
