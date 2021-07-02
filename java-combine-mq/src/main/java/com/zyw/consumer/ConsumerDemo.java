package com.zyw.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class ConsumerDemo {

    public static void main(String[] args) throws Exception {
        // 1. 建立消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("zyw-consumer");
        // 2. 为消费者配置nameserver地址
        consumer.setNamesrvAddr("localhost:9876");

        // 3. 消费者订阅消息,这里第二个参数"*"表示接收的是所有消息
        consumer.subscribe("my-first-topic", "update");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
                                                            ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                try {
                    for (MessageExt msg : list) {
                        System.out.println("消息：" + new String(msg.getBody(), "UTF-8"));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                System.out.println("接收到消息 -> " + list);

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 4. 启动消费者
        consumer.start();
    }
}
