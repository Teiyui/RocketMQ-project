package com.zyw.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

public class OrderProducer {

    public static void main(String[] args) throws Exception {
        // 1. 创建一个生产者
        DefaultMQProducer producer = new DefaultMQProducer("zyw-Order-Group");
        // 2. 设置nameserver的地址
        producer.setNamesrvAddr("localhost:9876");
        // 3. 开启一个生产者
        producer.start();
//        producer.createTopic("broker-zyw", "zyw-order-topic", 10);
        for (int i = 0; i < 100; i++) {
            String msgStr = "order -->" + i;
            Message message = new Message("zyw-order-topic", "order-msg",
                    msgStr.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                // 第一个参数指当前topic下的消息队列，第二个参数指发送的消息，第三个参数指回调参数（值等同于send方法中的第三个参数）
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    Integer id = (Integer) o;
                    int index = id % list.size();
                    return list.get(index);
                }
            }, i);
//            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }
        producer.shutdown();
    }
}
