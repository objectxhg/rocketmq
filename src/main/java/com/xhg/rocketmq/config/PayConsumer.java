package com.xhg.rocketmq.config;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;

/**
 * @Author xiaoh
 * @create 2020/9/16 17:40
 */
@Component
@SuppressWarnings("all")
public class PayConsumer {


    private DefaultMQPushConsumer consumer;

    private String consumerGroup = "pay_consumer";

    public PayConsumer() throws MQClientException {
        /**
         * 创建队列
         */
        consumer = new DefaultMQPushConsumer(consumerGroup);
        /**
         * rocker 默认开启vip通道 vip端口为10909 若服务器未开放此端口 则设为false 不走vip通道 消费消息
         * 若已开放 则不设置
         */
        consumer.setVipChannelEnabled(false);
        // 指定服务端
        consumer.setNamesrvAddr(RocketConfig.NAME_SERVER);
        // 设置消费消费策略, 从最后一个进行消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        /**
         * 指定订阅的Topic主题标签 和 二级标签 二级标签设为 * 表示全部二级标签
         */
        consumer.subscribe(RocketConfig.TOPIC, "taga");
        // 注册监听器
        consumer.registerMessageListener((MessageListenerConcurrently)
                (msgs, context) -> {
                    try {
                        // 获取Message
                        Message msg = msgs.get(0);
                        System.out.printf("%s Receive New Messages: %s %n",
                                Thread.currentThread().getName(), new String(msgs.get(0).getBody()));
                        String topic = msg.getTopic();
                        String body = new String(msg.getBody(), "utf-8");
                        // 标签
                        String tags = msg.getTags();
                        String keys = msg.getKeys();
                        System.out.println("topic=" + topic + ", tags=" + tags + ",keys=" + keys + ", msg=" + body);
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                });
        consumer.start();
        System.out.println("Consumer Listener");
    }
}

