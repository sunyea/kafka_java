import com.kds.kafka.Kafka;
import org.json.JSONObject;

public class ThreadHandler implements Runnable {
    String hosts = "192.168.100.70:9092,192.168.100.71:9092,192.168.100.72:9092";
    String in_topic = "tp.test.common";
    String out_topic = "tp.test.common.response";
    String groupid = "gp.java.common";

    private String message;
    private Thread t;
    private Kafka kafka;

    ThreadHandler(String message) {
        this.message = message;
        kafka = new Kafka(hosts);
        kafka.start(out_topic);
    }
    @Override
    public void run() {
        System.out.println("++++++处理消息线程开启");
        String rt;
        try {
            JSONObject js_msg = new JSONObject(message);
            String action = js_msg.getString("action");
            String sessionid = js_msg.getString("sessionid");
            JSONObject data = js_msg.getJSONObject("data");

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
        System.out.println("处理结果："+rt);
        //发送到kafka
        try {
            kafka.send_always(rt);
            System.out.println("发送成功");
        }catch (Exception e) {
            System.out.println("发送失败："+e.getMessage());
        }finally {
            kafka.stop();
        }
        System.out.println("======处理消息线程结束");
    }

    public void start() {
        if (t == null) {
            t = new Thread(this, "kafka_handler");
            t.start();
        }
    }
}
