package com.zyw.process.send;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * 发送消息（异步）
 */
public class AsyncProducer {

    public static void main(String[] args) throws Exception {
        // 1. 创建一个生产者
        DefaultMQProducer producer = new DefaultMQProducer("zyw-Group");
        // 2. 设置nameserver的地址
        producer.setNamesrvAddr("localhost:9876");
        // 3. 启动生产者
        producer.start();

        // 4. 发送消息
        String msg = "我的第二个异步发送消息!";
        Message message = new Message("my-first-topic", "update", msg.getBytes("UTF-8"));
        producer.send(message, new SendCallback() {
            public void onSuccess(SendResult sendResult) {
                System.out.println("发送第二个异步消息成功");
                System.out.println("消息id：" + sendResult.getMsgId());
                System.out.println("消息队列：" + sendResult.getMessageQueue());
                System.out.println("消息offset值：" + sendResult.getQueueOffset());
                System.out.println(sendResult);
            }

            public void onException(Throwable throwable) {
                System.out.println("消息发送失败" + throwable);
            }
        });

//        producer.shutdown();
    }
}
