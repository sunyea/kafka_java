package com.kds.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Kafka {
    private String hosts;
    protected Logger loger;
    private int max_buf;
    private int max_retry;
    private int timeout;
    private boolean debug;
    private String in_topic;
    private String out_topic;
    private String groupid;
    private boolean run;
    private Producer<String, String> producer;
    private Consumer<String, String> consumer;

    private ConsumerRecords<String, String> _records;

    /**
     * 构造函数
     * @param hosts: kafka服务器地址，多个地址用逗号隔开
     * @param max_buf: 最大收发信息量
     * @param max_retry: 消费者超时重试次数
     * @param timeout: 消费者超时（毫秒）
     * @param debug: 是否是调试模式
     */
    public Kafka(String hosts, int max_buf, int max_retry, int timeout, boolean debug) {
        this.hosts = hosts;
        this.run = false;
        this.loger = Logger.getLogger(Kafka.class);
        this.max_buf = max_buf;
        this.max_retry = max_retry;
        this.timeout=timeout;
        this.debug = debug;
        if (this.debug) {
            this.loger.debug("创建Kafka实例，并初始化.");
        }
    }
    public Kafka(String hosts) {
        this(hosts, 999950, 4, 6000, true);
    }

    /**
     * 初始化Producer
     * @param out_topic 输出主题
     * @return 如果初始化成功返回true
     */
    private boolean init_producer(String out_topic) {
        if (out_topic.isEmpty()) {
            this.loger.error("主题名为空值");
            return false;
        }
        this.out_topic = out_topic;
        try {
            if (this.debug)
                this.loger.info("开始初始化Producer...");
            Properties prop_producer = new Properties();
            prop_producer.put("bootstrap.servers", this.hosts);
            prop_producer.put("acks", "all");
            prop_producer.put("linger.ms", 0);
            prop_producer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            prop_producer.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            this.producer = new KafkaProducer<String, String>(prop_producer);
            if (this.debug)
                this.loger.info("Producer完成初始化...");
            return true;
        }catch (Exception e) {
            this.loger.error(String.format("创建producer异常：%s", e.getMessage()));
            return false;
        }
    }

    private boolean init_consumer(String in_topic, String consumer_group) {
        if (in_topic.isEmpty()) {
            this.loger.error("主题名为空值");
            return false;
        }
        this.in_topic = in_topic;
        if (consumer_group.isEmpty()) {
            this.loger.error("消费者组名为空");
            return false;
        }
        this.groupid = consumer_group;
        try {
            if (this.debug)
                this.loger.info("开始初始化Consumer...");
            Properties prop_consumer = new Properties();
            prop_consumer.put("bootstrap.servers", this.hosts);
            prop_consumer.put("group.id", this.groupid);
            prop_consumer.put("auto.offset.reset", "earliest");
            prop_consumer.put("enable.auto.commit", false);
            prop_consumer.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            prop_consumer.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            this.consumer = new KafkaConsumer<String, String>(prop_consumer);
            this.consumer.subscribe(Arrays.asList(this.in_topic));
            if (this.debug)
                this.loger.info("Consumer完成初始化...");
            return true;
        }catch (Exception e) {
            this.loger.error(String.format("创建consumer异常：%s", e.getMessage()));
            return false;
        }
    }

    /**
     * 开始启动kafka对象
     * @param in_topic: 输入信息的主题名
     * @param out_topic: 输出信息的主题名
     * @param consumer_group: 消费者组群名
     * @return: 正确启动返回true
     */
    public boolean start(String in_topic, String out_topic, String consumer_group) {
        if (this.run){
            this.loger.warn("启动失败，因为kafka实例已经是启动状态");
            return false;
        }
        if(!(this.init_producer(out_topic))) {
            return false;
        }
        if (!(this.init_consumer(in_topic, consumer_group))) {
            return false;
        }
        this.run = true;
        this.loger.info("**** START **** 启动了Kafka实例。");
        return true;
    }

    public boolean start(String in_topic, String consumer_group) {
        if (this.run){
            this.loger.warn("启动失败，因为kafka实例已经是启动状态");
            return false;
        }
        if (!(this.init_consumer(in_topic, consumer_group))) {
            return false;
        }
        this.run = true;
        this.loger.info("**** START **** 启动了Kafka实例。");
        return true;
    }

    public boolean start(String out_topic) {
        if (this.run){
            this.loger.warn("启动失败，因为kafka实例已经是启动状态");
            return false;
        }
        if(!(this.init_producer(out_topic))) {
            return false;
        }
        this.run = true;
        this.loger.info("**** START **** 启动了Kafka实例。");
        return true;
    }

    /**
     * 停止kafka对象
     */
    public void stop() {
        this.run = false;
        if(this.producer != null) {
            this.producer.close();
        }
        if(this.consumer != null) {
            this.consumer.close();
        }
        this.loger.info("**** STOP **** 停止了Kafka实例。");
    }

    /**
     * 生产者推送消息到kafka
     * @param message 被推送的消息
     * @return 正确执行返回true
     */
    private boolean producer_send(String message) {
        int len_msg = message.length();
        if (len_msg > this.max_buf) {
            this.loger.error(String.format("消息大小为：%d,超过了系统限制的%d字节", len_msg, this.max_buf));
            return false;
        }
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(this.out_topic, message);
            this.producer.send(record);
            return true;
        }catch (Exception e) {
            this.loger.error("发送信息失败，原因："+e.getMessage());
            return false;
        }
    }

    /**
     * 消费者从kafka获取信息
     * @return 返回获取到的信息，如果无信息或者异常返回null
     */
    private String consumer_get() {
        try {
            ConsumerRecords<String, String> records = this.consumer.poll(this.timeout);
            for(ConsumerRecord<String, String> record: records) {
                String msg = record.value();
                if (msg.isEmpty()) {
                    continue;
                }
                if (this.debug) {
                    this.loger.info(String.format("收到指令：%s", msg));
                }
                return msg;
            }
            return null;
        }catch (Exception e) {
            this.loger.error("接收信息失败，原因："+e.getMessage());
            return null;
        }
    }

    private String get_session(String message) {
        JSONObject js_msg = new JSONObject(message);
        return js_msg.getString("sessionid");
    }

    /**
     *  处理远程请求命令，在具体应用中重载该方法进行具体业务逻辑处理
     * @param message: 远程请求消息，json字符串，格式：{'action':'command', 'sessionid':'****', 'data':{...}}
     * @return: 业务处理后的反馈信息，json字符串，格式：{'code':0, 'err':'info', 'sessionid':'****', 'data':{...}}
     */
    public String command(String message) {
        JSONObject js_msg = new JSONObject(message);
        String action = js_msg.getString("action");
        String sessionid = js_msg.getString("sessionid");
        JSONObject data = js_msg.getJSONObject("data");
        String rt;
        if (action.isEmpty()) {
            rt = String.format("{'code': -1, 'err': '没有明确的指令', 'sessionid': '%s', 'data': %s}",
                    sessionid, data.toString());
        }else if (action.equals("resp")) {
            rt = String.format("{'code': 0, 'err': '%s 执行成功', 'sessionid': '%s', 'data': %s}",
                    action, sessionid, data.toString());
        }else{
            rt = String.format("{'code': -2, 'err': '未知指令 %s', 'sessionid': '%s', 'data': %s}",
                    action, sessionid, data.toString());
        }
        return rt;
    }

    /**
     * 等待处理远程请求命令
     */
    public void waitForAction() throws Exception {
        while(this.run) {
            String message = this.consumer_get();
            if (message == null) {
                continue;
            }
            String resp_msg = this.command(message);
            if(!(this.producer_send(resp_msg))){
                throw new Exception("发送反馈信息失败");
            }
            this.consumer.commitSync();
            if (debug) {
                this.loger.info(String.format("反馈指令：%s", resp_msg));
            }
        }
    }

    /**
     * 发送远程请求指令，并等待指令结果
     * @param message: 发送给远程的指令，json字符串，格式：{'action':'command', 'sessionid':'****', 'data':{...}}
     * @return: 指令结果，json字符串，格式：{'code':0, 'err':'info', 'sessionid':'****', 'data':{...}}
     */
    public String requestAndResponse(String message) throws Exception {
        String sessionid = this.get_session(message);
        if(sessionid.isEmpty()) {
            this.loger.error("待发送的信息没有sessionid，发送失败");
            return "{'code': -1, 'err': 'sessionid异常', 'sessionid': null, 'data': null}";
        }
        if(!(this.producer_send(message))) {
            this.loger.error("消息发送失败");
            return String.format("{'code': -2, 'err': '消息发送失败', 'sessionid': '%s', 'data': null}",
                    sessionid);
        }
        for (int c_count = 0; c_count < this.max_retry; c_count++) {
            while(true) {
                String msg_payload = this.consumer_get();
                if (msg_payload == null) {
                    this.loger.warn(String.format("获取超时，尝试第%d次再次获取.", c_count+1));
                    continue;
                }
                String resp_sessionid = this.get_session(msg_payload);
                if (resp_sessionid.equals(sessionid)) {
                    return msg_payload;
                }
            }
        }
        this.loger.error("接收反馈超时失败");
        return String.format("{'code': -4, 'err': '接收反馈超时失败', 'sessionid': '%s', 'data': null}",
                sessionid);
    }

    /**
     * 可持续发送信息
     * @param message: 信息内容
     * @throws Exception
     */
    public void send_always(String message) throws Exception {
        this.producer_send(message);
    }

    /**
     * 可持续接收信息
     * @return 收到的信息内容
     * @throws Exception
     */
    public String get_always() throws Exception {
        String message = this.consumer_get();
        if (message != null)
            this.consumer.commitSync();
        return message;
    }

    /**
     * 发送单条信息到指定主题
     * @param topic: 主题名
     * @param message: 信息内容
     * @return: 如果成功返回true
     */
    public boolean send_msg(String topic, String message) {
        try {
            if(!(this.start(topic))) {
                return false;
            }
            if(!(this.producer_send(message))){
                return false;
            }
            this.stop();
            return true;
        }catch (Exception e) {
            this.loger.error("发送失败，原因："+e.getMessage());
            return false;
        }
    }

    /**
     * 从指定主题获取一条信息
     * @param topic: 主题名
     * @param consumer_group: 消费者组群
     * @return: 获取的信息，如果未获取返回null
     */
    public String get_msg(String topic, String consumer_group) {
        try {
            if(!(this.start(topic, consumer_group))) {
                return null;
            }
            String msg = this.consumer_get();
            this.stop();
            return msg;
        }catch (Exception e) {
            this.loger.error("获取信息失败，原因："+e.getMessage());
            return null;
        }
    }

}
