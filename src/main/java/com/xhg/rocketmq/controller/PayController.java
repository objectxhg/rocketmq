package com.xhg.rocketmq.controller;

import com.xhg.rocketmq.config.PayProducer;
import com.xhg.rocketmq.config.RocketConfig;
import com.xhg.rocketmq.threadPool.service.AsyncTaskService;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.UnsupportedEncodingException;


/**
 * @Author xiaoh
 * @create 2020/9/16 11:45
 */
@RestController
@SuppressWarnings("all")
public class PayController {

    @Autowired
    private AsyncTaskService asyncTaskService;

    /**
     * https://www.jianshu.com/p/23b4f6bff320
     * RocketMQ的消息存储是由consume queue和Commit Log配合完成的。
     *
     * consume queue是消息的逻辑队列，相当于字典的目录，存放的是消息的索引位置 用于指定消息在物理文件Commit Log中的位置 顺序写顺序读
     *
     * Commit Log是消息存放的物理文件，每台Broker上的Commit Log被本机所有的queue共享 文件顺序写，随机读
     * Broker端在收到一条消息后，如果是消息需要落盘，则会在Commit Log中写入整条消息
     * 并在consume queue中写入该消息的索引信息
     * 消息被消费时，则根据consume queue中的信息去Commit Log中获取消息。RocketMQ在消息被消费后，并不会去Commit Log中删除消息，而是会保存3天（可配置）而后批量删除。
     *
     * RocketMQ支持同步刷盘及异步刷盘两种模式，同步刷盘指的是Producer将消息发送至Broker后，等待消息刷入Commit Log和consume queue后才算消息发送成功，
     * 而异步刷盘则是将消息发送至Broker后，Broker将消息放入内存后立马告知Producer消息发送成功，而后由Broker自行将内存中的消息批量刷入磁盘
     *
     */

    @RequestMapping("/api/paymq")
    public String callback(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException, UnsupportedEncodingException {
        /**
         * 创建消息:
         *  Topic主题
         *  二级标签
         *  消息内容字节数组
         *
         * 向broker 发送主题信息、二级标签、消息内容
         *
         * broker将 消息写入 Commit Log 中并在consume queue中写入该消息的索引信息
         *
         * 当消息被消费时 根据consume queue的索引信息去Commit Log获取消息
         *
         * 消费完成后Commit Log中的消息不会被立马删除，而是会保存3天（可配置）然后批量删除。
         *
         */
        Message message = new Message(RocketConfig.TOPIC, "taga", ("hello rocketMQ " + text).getBytes());

        asyncTaskService.sendMQAsyncTask(message);

        System.out.println("发送成功");

        return "发送成功";
    }

}

