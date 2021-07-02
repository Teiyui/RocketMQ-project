package com.zyw.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class OrderConsumer {

    public static void main(String[] args) throws Exception {
        // 1. 建立消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("zyw-consumer");
        // 2. 为消费者配置nameserver地址
        consumer.setNamesrvAddr("localhost:9876");
        // 3. 消费者订阅消息，这里第二个参数根据用户的属性信息进行订阅消息
        consumer.subscribe("zyw-order-topic", "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
                                                            ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                try {
                    for (MessageExt msg : list) {
                        System.out.println(Thread.currentThread().getName() + " queue Id is: " +
                                msg.getQueueId() + "消息内容体为当前订单编号" +new String(msg.getBody(), "UTF-8"));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 4. 启动消费者
        consumer.start();
    }
}
