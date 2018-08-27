Kafka应用封装类，基于Java8
==

# 1. QuickStart
## 1.1. 包导入
将Kfka.class拷贝到项目中，在项目中使用：<br>
```
import com.kds.kafka.Kafka;
```
导入头文件后即可使用

## 1.2. 新建子类
在项目新建Java类（如：ServerKafka），继承自Kafka类，并实现构造函数，必须重载command方法<br>
```
public String command(String message) {
    ...
}
```
重载的command方法来完成waitForAction()方法具体的业务逻辑，如果不需要使用waitForAction方法，可以不创建新的子类。

## 1.3. 使用实例
要使用Kafka实例(或子类实例)时，先进行对象创建，使用:
```
Kafka kafka = new Kafka(hosts);
```
创建时需要提供hosts参数，表示kafka的服务列表，可以跟多个IP:port,IP:port的方式，中间以逗号隔开。<br>
在使用实例之前，要先调用start()初始化，使用结束后调用stop()清理。
```
kafka.start(...);
//具体业务代码
kafka.stop();
```

## 1.4. 服务程序实现
具体业务需求：等待前端的请求，并进行相应业务处理，将结果返回给前端。<br>
* 首先新建Kafka子类，重载command方法来实现对前端请求的解析及业务处理;
* 然后在服务主程序中创建该子类对象，并使用start()进行初始化;
* 使用while(true)建立消息循环，在循环体中使用waitForAction()处理前端请求;
* 当处理请求出现异常，先用stop()停止对象，然后使用start()重启对象，并继续处理前端请求;
```
Kafka kafka = new Kafka(hosts);
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
```

## 1.5. 调用其他服务
具体业务需求：发起一个远程请求，并等待回馈。<br>
* 创建Kafka对象，使用start()进行初始化;
* 使用requestAndResponse()发起请求，并得到回馈;
* 使用完之后用stop()清理资源并结束。

## 1.6. 发送一批信息到kafka
具体业务需求：将一批信息发送到同一个主题。<br>
* 创建Kafka对象，使用start()进行初始化;
* 使用send_always()发送一条信息，如需发送多条，则多次调用该方法;
* 使用完之后用stop()清理资源并结束。

## 1.7. 从kafka获取一批信息
具体业务需求：从同一个主题获取一批信息。<br>
* 创建Kafka对象，使用start()进行初始化;
* 使用get_always()接收一条信息，如需接收多条，则多次调用该方法;
* 使用完之后用stop()清理资源并结束。

## 1.8. 单条信息处理的简洁方法
具体业务需求：针对指定主题处理单条信息（接收或发送）。<br>
* 创建Kafka对象
* send_msg方法可以发送单条信息
* get_msg方法可以接收单挑信息
* 使用简洁方法不需要用start()和stop()方法，但是只针对单条信息使用。

# 2. 依赖包
使用Kafka类时，依赖的开发包有：org.apache.kafka, org.json, org.apache.log4j三个。
## 2.1. org.apache.kafka
kafka的java开发包，属于第三方包，需要添加依赖<br>
基于maven的包依赖：<br>
* 在IntelliJ中点击"Project Structure"->"Modules"->+"Library"->"NewLibrary"->"From Maven"->search "org.apache.kafka"
* 等待搜索结果，然后确认。
* 查看左侧项目浏览器中的External Libraries是否有org.apache.kafka:kafka_*.*:*，如果有表示添加成功。

## 2.2. org.json
处理json字符串的包，属于第三方包，需要添加依赖，添加方法同kafka。

## 2.3. org.apache.log4j
Java中的日志记录包，属于第三方，但是IntelliJ环境建立Java项目就自动依赖了该包<br>
不需要额外管理，如果使用的其他IDE，需要添加依赖。