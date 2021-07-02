package com.zyw.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * 发送消息（同步）
 */
public class SyncProducer {

    public static void main(String[] args) throws Exception {
        // 1. 创建一个生产者
        DefaultMQProducer producer = new DefaultMQProducer("zyw-Group");
        // 2. 设置nameserver的地址
        producer.setNamesrvAddr("localhost:9876");
        // 3. 启动生产者
        producer.start();

        // 4. 发送消息
        String msg = "这是一个用户的消息";
        Message message = new Message("my-topic-filter", msg.getBytes("UTF-8"));

        // 5. 用户自定义属性
        message.putUserProperty("sex", "女");
        message.putUserProperty("age", "18");
        SendResult result = producer.send(message);
        System.out.println("消息id：" + result.getMsgId());
        System.out.println("消息队列：" + result.getMessageQueue());
        System.out.println("消息offset值：" + result.getQueueOffset());
        System.out.println(result);

        producer.shutdown();
    }
}
