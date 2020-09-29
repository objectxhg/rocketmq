package com.xhg.rocketmq.threadPool.service;

import com.xhg.rocketmq.config.PayProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;

/**
 * @Author xiaoh
 * @create 2020/9/29 15:15
 */
@Component
public class AsyncTaskService {

    @Autowired
    private PayProducer payProducer;

    @Async
    public void sendMQAsyncTask(Message message) throws InterruptedException, RemotingException, MQClientException, MQBrokerException, UnsupportedEncodingException {

        System.out.println("线程" + Thread.currentThread().getName() + " 执行异步任务：" + new String(message.getBody(), "utf-8"));
        payProducer.getProducer().send(message);
    }
}

