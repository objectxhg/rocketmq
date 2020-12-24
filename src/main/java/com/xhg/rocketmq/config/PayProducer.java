package com.xhg.rocketmq.config;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.stereotype.Component;

/**
 * @Author xiaoh
 * @create 2020/9/16 11:38
 */
@Component
@SuppressWarnings("all")
public class PayProducer {

    /**
     * 生产组,生产者必须在生产组内
     */
    private String producerGroup = "broker-a"; //须更名为生产者 pay_producer


    private DefaultMQProducer producer;

    public PayProducer() {
        producer = new DefaultMQProducer(producerGroup);
        /**
         * rocker 默认开启vip通道 vip端口为10909 若服务器未开放此端口 则设为false 不走vip通道发送消息
         * 若已开放 则不设置
         */
        producer.setVipChannelEnabled(false);
        /**
         * 指定nameServer地址,多个地址(集群)之间用 ; 隔开
         * 注册 Broker  定时向NameServer提供主题信息,告诉NameServer我这里可以把消息传落盘和传输给消费者Consumer
         */
        producer.setNamesrvAddr(RocketConfig.NAME_SERVER);
        start();
    }

    public DefaultMQProducer getProducer() {
        return producer;
    }

    /**
     * 对象在使用之前必须调用一次,并且只能初始化一次
     */
    public void start() {
        try {
            this.producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * 一般在应用上下文,使用上下文监听器,进行关闭
     */
    public void shutdown() {
        producer.shutdown();
    }
}

