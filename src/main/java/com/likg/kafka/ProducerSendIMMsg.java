package com.likg.kafka;

import com.alibaba.fastjson.JSONObject;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang.StringUtils;

import java.util.Properties;
import java.util.Random;

/**
 *
 */
public class ProducerSendIMMsg {

    public static void main(String[] args) throws InterruptedException {
        Random rnd = new Random();
        int events = 1;

        // 设置配置属性
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.1.208:9092");
        //props.put("metadata.broker.list", "172.17.0.127:9092");
        props.put("kafka.topic", "slow");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // key.serializer.class默认为serializer.class
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        // 可选配置，如果不配置，则使用默认的partitioner
        //props.put("partitioner.class", "com.catt.kafka.demo.PartitionerDemo");
        // 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
        // 值为0,1,-1,可以参考
        // http://kafka.apache.org/08/configuration.html
        props.put("request.required.acks", "1");
        props.put("producer.type", "sync");


        // 异步模式下缓冲数据的最大时间。例如设置为100则会集合100ms内的消息后发送，这样会提高吞吐量，但是会增加消息发送的延时
        props.put("queue.buffering.max.ms", "10");
        // 异步模式下缓冲的最大消息数，同上
        //queue.buffering.max.messages = 10000
        // 异步模式下，消息进入队列的等待时间。若是设置为0，则消息不等待，如果进入不了队列，则直接被抛弃
        //queue.enqueue.timeout.ms = -1
        // 异步模式下，每次发送的消息数，当queue.buffering.max.messages或queue.buffering.max.ms满足条件之一时producer会触发发送。
        props.put("batch.num.messages", "200");

        ProducerConfig config = new ProducerConfig(props);

        // 创建producer
        Producer<String, Object> producer = new Producer<>(config);
        // 产生并发送消息
        long start = System.currentTimeMillis();

        //
        String uid = "11";
        String[] toUidList = {"100144"};
        int objectName = 54; //推送任务
        String taskId = "13";

        JSONObject dataObj = new JSONObject();
        dataObj.put("uid", uid);
        dataObj.put("toUid", StringUtils.join(toUidList, ","));
        dataObj.put("objectName", String.valueOf(objectName));

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("taskId", taskId);
        JSONObject content = new JSONObject();
        content.put("endTime", 0);
        content.put("is_redpacket_task", 0);
        jsonObject.put("content", content.toJSONString());

        dataObj.put("content", jsonObject.toJSONString());
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("btype", "system");
        jsonObj.put("data", dataObj);

        KeyedMessage data2 = new KeyedMessage("sysmsg", uid, jsonObj.toJSONString());
        producer.send(data2);

        System.out.println("耗时:" + (System.currentTimeMillis() - start));
        // 关闭producer
        producer.close();
    }
}