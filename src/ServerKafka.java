import com.kds.kafka.Kafka;
import org.json.JSONObject;

public class ServerKafka extends Kafka {
    public ServerKafka(String hosts) {
        super(hosts);
    }

    public String command(String message) {
        try {
            JSONObject js_msg = new JSONObject(message);
            String action = js_msg.getString("action");
            String sessionid = js_msg.getString("sessionid");
            JSONObject data = js_msg.getJSONObject("data");

            String rt;
            if (action.isEmpty()) {
                rt = String.format("{'code': -1, 'err': '[Java]没有明确的指令', 'sessionid': '%s', 'data': %s}",
                        sessionid, data.toString());
            }else if (action.equals("resp")) {
                rt = String.format("{'code': 0, 'err': '[Java]%s 执行成功', 'sessionid': '%s', 'data': %s}",
                        action, sessionid, data.toString());
            }else{
                rt = String.format("{'code': -2, 'err': '[Java]未知指令 %s', 'sessionid': '%s', 'data': %s}",
                        action, sessionid, data.toString());
            }
            return rt;
        }catch (Exception e){
            return String.format("{'code': -3, 'err': '[Java]异常 (%s)', 'sessionid': null, 'data': null}",
                    e.getMessage());
        }
    }
}
