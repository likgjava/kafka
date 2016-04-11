package com.likg.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 详细可以参考：https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example
 */
public class ConsumerDemo {
    private final ConsumerConnector consumer;
    private final String topic;
    private static ExecutorService executor;
    public static long count;

    public ConsumerDemo(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }

    public void shutdown() {
        if (consumer != null) {
            consumer.shutdown();
        }

        if (executor != null) {
            executor.shutdown();
        }
    }

    public void run(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all the threads
        executor = Executors.newFixedThreadPool(numThreads);

        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerMsgTask(stream, threadNumber));
            threadNumber++;
        }
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        //props.put("fetch.wait.max.ms", "1");
        //props.put("fetch.min.bytes", "6553600");

        return new ConsumerConfig(props);
    }

    public static void main(String[] arg) {
        String zooKeeper = "172.17.0.9:2181,172.17.0.10:2181,172.17.0.11:2181/config/mobile/mq";
        String topic = "msg_push";
        String groupId = "group-push";
        int threads = 5;

        long time = System.currentTimeMillis();

        ConsumerDemo demo = new ConsumerDemo(zooKeeper, groupId, topic);
        demo.run(threads);

        //启动一次顺序关闭，执行以前提交的任务，但不接受新任务
        executor.shutdown();
        //等待线程终止
        while(!executor.isTerminated()){
        }

        time = System.currentTimeMillis() - time;
        System.out.println("times==ms===" + time);
        demo.shutdown();

        //1574930
    }
}