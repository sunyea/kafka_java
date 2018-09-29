import java.util.ArrayList;
import java.util.List;

public class KafkaTest {
    public static void main(String[] args) {
        String hosts = "192.168.100.70:9092,192.168.100.71:9092,192.168.100.72:9092";
        String in_topic = "tp.test.io";
        String out_topic = "tp.test.io";
        String groupid = "gp.test.java";


        long start = System.currentTimeMillis();

        long end = System.currentTimeMillis();

        System.out.println(String.format("信息100条，耗时：%d",end-start));
    }
}
