package com.likg.kafka.util;

import java.util.List;

public class KafkaLagMonitor {


    private static final String kafka_broker_list = "192.168.1.238";
    private static final int kafka_broker_port = 9092;
    private static final String zookeeper_connect = "192.168.1.238:2181/config/mobile/mq";
    private static final String topic_name = "stat";
    private static final String group_id = "group-stat";


    public static void main(String[] args) {
        //初始化
        ZkUtils.init(zookeeper_connect);

        //获取分区列表
        List<Integer> partitionIdList = KafkaHelper.getPartitionIdList(kafka_broker_list, kafka_broker_port, topic_name);
        System.out.println("partitionIdList=" + partitionIdList);

        for (Integer partitionId : partitionIdList) {
            //获取logSize
            long logSize = KafkaHelper.getLogSize(kafka_broker_list, kafka_broker_port, topic_name, partitionId);

            //获取offset
            long offset = getOffset(topic_name, group_id, partitionId);

            System.out.println("partitionId=" + partitionId + " logSize=" + logSize + " offset=" + offset);

        }


    }

    private static long getOffset(String topicName, String groupId, Integer partitionId) {
        String path = "/consumers/" + groupId + "/offsets/" + topicName + "/" + partitionId;
        String content = ZkUtils.getContent(path);
        //System.out.println("content=" + content);
        if (content == null) {
            throw new RuntimeException("content is null");
        }
        return Long.parseLong(content);
    }


}
