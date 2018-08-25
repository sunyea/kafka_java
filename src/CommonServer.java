import org.apache.log4j.Logger;

public class CommonServer {
    public static void main(String[] args) {
        String hosts = "192.168.100.70:9092,192.168.100.71:9092,192.168.100.72:9092";
        String in_topic = "tp.test.common";
        String out_topic = "tp.test.common.response";
        String groupid = "gp.java.common";

        Logger loger = Logger.getLogger(CommonServer.class);
        loger.info("准备开始启动通用服务...");
        ServerKafka kafka = new ServerKafka(hosts);
        if (kafka.start(in_topic, out_topic, groupid)) {
            while (true) {
                try {
                    kafka.waitForAction();
                }catch (Exception e) {
                    loger.error(String.format("kafka发生异常(%s)，马上重启...", e.getMessage()));
                    kafka.stop();
                    kafka.start(in_topic, out_topic, groupid);
                }
            }
        }
        loger.info("通用服务停止运行.");
    }
}
