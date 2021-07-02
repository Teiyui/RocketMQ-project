package com.zyw.topic;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

public class TopicDemo {

    public static void main(String[] args) throws MQClientException {
        // 1. 创建一个生产者
        DefaultMQProducer producer = new DefaultMQProducer("zyw-Group");
        // 2. 设置nameserver的地址
        producer.setNamesrvAddr("localhost:9876");
        // 3. 启动生产者
        producer.start();
        /**
         * 创建topic，参数分别是：broker的名称，topic的名称，queue的数量
         *
         * queue的数量默认为4
         */
        producer.createTopic("broker-zyw", "my-first-topic", 8);
        System.out.println("topic创建成功");
        // 4. 关闭生产者
        producer.shutdown();
    }
}
