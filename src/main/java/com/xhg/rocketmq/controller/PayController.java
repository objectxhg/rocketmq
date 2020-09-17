package com.xhg.rocketmq.controller;

import com.xhg.rocketmq.config.PayProducer;
import com.xhg.rocketmq.config.RocketConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * @Author xiaoh
 * @create 2020/9/16 11:45
 */
@RestController
@SuppressWarnings("all")
public class PayController {

    @Autowired
    private PayProducer payProducer;


    @RequestMapping("/api/paymq")
    public String callback(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        /**
         * 创建消息:
         *  Topic主题
         *  二级标签
         *  消息内容字节数组
         */
        Message message = new Message(RocketConfig.TOPIC, "taga", ("hello rocketMQ " + text).getBytes());

        SendResult send = payProducer.getProducer().send(message);

        System.out.println(send);

        return "发送成功";
    }

}

