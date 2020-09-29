package com.xhg.rocketmq.config;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

/**
 * @Author xiaoh
 * @create 2020/9/16 11:38
 */
@Component
@SuppressWarnings("all")
public class PayTransactionProducer implements TransactionListener {

    /**
     * 生产组,生产者必须在生产组内
     */
    private String producerGroup = "broker-a";


    private TransactionMQProducer transactionProducer;

    public PayTransactionProducer() {
        transactionProducer = new TransactionMQProducer(producerGroup);
        /**
         * rocker 默认开启vip通道 vip端口为10909 若服务器未开放此端口 则设为false 不走vip通道发送消息
         * 若已开放 则不设置
         */
        transactionProducer.setVipChannelEnabled(false);
        /**
         * 指定nameServer地址,多个地址之间以 ; 隔开
         * 注册 broker
         */
        transactionProducer.setNamesrvAddr(RocketConfig.NAME_SERVER);
        start();
    }

    public TransactionMQProducer getTransactionProducer() {
        return transactionProducer;
    }

    /**
     * 对象在使用之前必须调用一次,并且只能初始化一次
     */
    public void start() {
        try {
            this.transactionProducer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * 一般在应用上下文,使用上下文监听器,进行关闭
     */
    public void shutdown() {
        transactionProducer.shutdown();
    }


    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        System.out.println("【mq消息回调】" + "getTags:" + message.getTags() + "Body:" + message.getBody());
        return null;

    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        System.out.println("【mq消息回调】" + "getTags:" + messageExt.getTags() + "Body:" + messageExt.getBody());
        return null;
    }
}

