import com.kds.kafka.Kafka;
import org.apache.log4j.Logger;

public class ThreadServer {
    public static void main(String[] args) {
        String hosts = "192.168.100.70:9092,192.168.100.71:9092,192.168.100.72:9092";
        String in_topic = "tp.test.common";
        String out_topic = "tp.test.common.response";
        String groupid = "gp.java.common";
        Logger loger = Logger.getLogger(ThreadServer.class);
        loger.info("准备开始启动多线程测试服务...");
        Kafka kafka = new Kafka(hosts);
        if(kafka.start(in_topic, groupid)) {
            while (true) {
                try {
                    String message = kafka.get_always();
                    if (message != null) {
                        //开启线程处理消息
                        ThreadHandler thHandler = new ThreadHandler(message);
                        thHandler.start();
                    }
                }catch (Exception e) {
                    loger.error(String.format("kafka发生异常(%s)，马上重启...", e.getMessage()));
                    kafka.stop();
                    kafka.start(in_topic, out_topic, groupid);
                }
            }
        }

        loger.info("多线程服务停止运行.");
    }
}
