import org.apache.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;

public class QueueServer {
    public static void main(String[] args) {
        Logger loger = Logger.getLogger(ThreadServer.class);
        loger.info("准备开始启动消息队列测试服务...");

        LinkedBlockingQueue<String> Que = new LinkedBlockingQueue<String>();

        ThreadGet th_get1 = new ThreadGet(Que);
        ThreadGet th_get2 = new ThreadGet(Que);
        ThreadSend th_send1 = new ThreadSend(Que);
        ThreadSend th_send2 = new ThreadSend(Que);

        th_get1.start();
        th_get2.start();
        th_send1.start();
        th_send2.start();

        loger.info("消息队列服务停止运行.");
    }
}
