import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class ThreadSend implements Runnable {
    String hosts = "192.168.100.70:9092,192.168.100.71:9092,192.168.100.72:9092";
    String out_topic = "tp.test.common.response";
    Logger loger = Logger.getLogger(ThreadServer.class);
    private Thread t;
    LinkedBlockingQueue<String> queue;
    KafkaProducer<String, String> producer;

    ThreadSend(LinkedBlockingQueue<String> queue) {
        try {
            this.loger.info("开始初始化Producer...");
            this.queue = queue;
            Properties prop_producer = new Properties();
            prop_producer.put("bootstrap.servers", this.hosts);
            prop_producer.put("acks", "all");
            prop_producer.put("linger.ms", 0);
            prop_producer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            prop_producer.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            this.producer = new KafkaProducer<String, String>(prop_producer);
            this.loger.info("Producer完成初始化...");
        }catch (Exception e) {
            this.loger.error(String.format("创建producer异常：%s", e.getMessage()));
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                String message = this.queue.take();
                String rt_msg = this.Handler(message);
                this.loger.info(String.format("发送反馈：%s", rt_msg));
                ProducerRecord<String, String> record = new ProducerRecord<>(this.out_topic, rt_msg);
                this.producer.send(record);
            }
        }catch (Exception e) {
            this.loger.error("发送信息失败，原因："+e.getMessage());
        }
    }

    private String Handler(String message) {
        String rt;
        try {
            JSONObject jmsg = new JSONObject(message);
            String action = jmsg.getString("action");
            String sessionid = jmsg.getString("sessionid");
            JSONObject data = jmsg.getJSONObject("data");

            if (action.isEmpty()) {
                rt = String.format("{'code': -1, 'err': '[Thread]没有明确的指令', 'sessionid': '%s', 'data': %s}",
                        sessionid, data.toString());
            }else if (action.equals("resp")) {
                rt = String.format("{'code': 0, 'err': '[Thread]%s 执行成功', 'sessionid': '%s', 'data': %s}",
                        action, sessionid, data.toString());
            }else{
                rt = String.format("{'code': -2, 'err': '[Thread]未知指令 %s', 'sessionid': '%s', 'data': %s}",
                        action, sessionid, data.toString());
            }
        }catch (Exception e){
            rt = String.format("{'code': -3, 'err': '[Java]异常 (%s)', 'sessionid': null, 'data': null}",
                    e.getMessage());
        }
        return rt;
    }

    public void start() {
        if (t == null) {
            t = new Thread(this, "thread_send");
            t.start();
        }
    }
}
