package com.likg.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 *
 */
public class ProducerDemo {

    public static void main(String[] args) throws InterruptedException {
        Random rnd = new Random();
        int events = 100000000;

        // 设置配置属性
        Properties props = new Properties();
        props.put("metadata.broker.list", "172.17.0.1:9092,172.17.0.2:9092,172.17.0.3:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // key.serializer.class默认为serializer.class
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        // 可选配置，如果不配置，则使用默认的partitioner
        //props.put("partitioner.class", "com.catt.kafka.demo.PartitionerDemo");
        // 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
        // 值为0,1,-1,可以参考
        // http://kafka.apache.org/08/configuration.html
        props.put("request.required.acks", "1");
        props.put("producer.type", "async");


        // 异步模式下缓冲数据的最大时间。例如设置为100则会集合100ms内的消息后发送，这样会提高吞吐量，但是会增加消息发送的延时
        props.put("queue.buffering.max.ms", "1000");
        // 异步模式下缓冲的最大消息数，同上
        //queue.buffering.max.messages = 10000
        // 异步模式下，消息进入队列的等待时间。若是设置为0，则消息不等待，如果进入不了队列，则直接被抛弃
        //queue.enqueue.timeout.ms = -1
        // 异步模式下，每次发送的消息数，当queue.buffering.max.messages或queue.buffering.max.ms满足条件之一时producer会触发发送。
        //batch.num.messages=200
        ProducerConfig config = new ProducerConfig(props);

        // 创建producer
        Producer<String, String> producer = new Producer<String, String>(config);
        // 产生并发送消息
        long start = System.currentTimeMillis();
        for (long i = 0; i < events; i++) {
            System.out.println("i==========================" + i);
            long runtime = new Date().getTime();
            String ip = "kk" + i;//rnd.nextInt(255);
            String msg = System.currentTimeMillis() + "";
            //如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("xm-msgbox", ip, msg);
            producer.send(data);
            Thread.sleep(1000);
        }
        System.out.println("耗时:" + (System.currentTimeMillis() - start));
        // 关闭producer
        producer.close();
    }
}