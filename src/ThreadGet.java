import org.apache.kafka.clients.consumer.*;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class ThreadGet  implements Runnable {
    String hosts = "192.168.100.70:9092,192.168.100.71:9092,192.168.100.72:9092";
    String in_topic = "tp.test.common";
    String groupid = "gp.java.common";
    Logger loger = Logger.getLogger(ThreadServer.class);
    private Thread t;
    LinkedBlockingQueue<String> queue;
    KafkaConsumer<String, String> consumer;

    ThreadGet(LinkedBlockingQueue<String> queue){
        try {
            this.loger.info("开始初始化Consumer...");
            this.queue = queue;
            Properties prop_consumer = new Properties();
            prop_consumer.put("bootstrap.servers", this.hosts);
            prop_consumer.put("group.id", this.groupid);
            prop_consumer.put("auto.offset.reset", "latest");
            prop_consumer.put("enable.auto.commit", false);
            prop_consumer.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            prop_consumer.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            this.consumer = new KafkaConsumer<String, String>(prop_consumer);
            this.consumer.subscribe(Arrays.asList(this.in_topic));
            this.loger.info("Consumer完成初始化...");
        }catch (Exception e) {
            this.loger.error(String.format("创建consumer异常：%s", e.getMessage()));
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = this.consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    String msg = record.value();
                    if (msg.isEmpty()) {
                        continue;
                    }
                    this.loger.info(String.format("收到指令：%s", msg));
                    this.queue.put(msg);
                }
                this.consumer.commitSync();
            }
        }catch (Exception e) {
            this.loger.error("接收信息失败，原因："+e.getMessage());
            this.consumer.close();
        }
    }

    public void start() {
        if (t == null) {
            t = new Thread(this, "thread_get");
            t.start();
        }
    }
}
