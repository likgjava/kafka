package com.likg.kafka.util;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaHelper {

    /**
     * 获取kafka logSize
     *
     * @param host
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    public static long getLogSize(String host, int port, String topic, int partition) {
        String clientName = "Client_" + topic + "_" + partition;
        Broker leaderBroker = getLeaderBroker(host, port, topic, partition);
        String reaHost = null;
        if (leaderBroker != null) {
            reaHost = leaderBroker.host();
            System.out.println("reaHost==" + reaHost);
        } else {
            System.out.println("Partition of Host is not find");
        }

        SimpleConsumer simpleConsumer = new SimpleConsumer(reaHost, port, 10000, 64 * 1024, clientName);
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = simpleConsumer.getOffsetsBefore(request);
        if (response.hasError()) {
            System.out.println("Error fetching data Offset , Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    public static void main(String[] args) {
        //long logSize = getLogSize("192.168.1.238", 9092, "stat", 0);
        //System.out.println(logSize);

        List<Integer> partitionIdList = getPartitionIdList("192.168.1.238", 9092, "stat");
        System.out.println(partitionIdList);

    }

    /**
     * 获取broker ID
     *
     * @param host
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    public static Integer getBrokerId(String host, int port, String topic, int partition) {
        Broker leaderBroker = getLeaderBroker(host, port, topic, partition);
        if (leaderBroker != null) {
            return leaderBroker.id();
        }
        return null;
    }

    /**
     * 获取leaderBroker
     *
     * @param host
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    private static Broker getLeaderBroker(String host, int port, String topic, int partition) {
        String clientName = "Client_Leader_LookUp";
        SimpleConsumer consumer = null;
        PartitionMetadata partitionMetaData = null;
        try {
            consumer = new SimpleConsumer(host, port, 10000, 64 * 1024, clientName);
            List<String> topics = new ArrayList<>();
            topics.add(topic);
            TopicMetadataRequest request = new TopicMetadataRequest(topics);
            TopicMetadataResponse response = consumer.send(request);
            List<TopicMetadata> topicMetadataList = response.topicsMetadata();
            for (TopicMetadata topicMetadata : topicMetadataList) {
                for (PartitionMetadata metadata : topicMetadata.partitionsMetadata()) {
                    if (metadata.partitionId() == partition) {
                        partitionMetaData = metadata;
                        break;
                    }
                }
            }
            if (partitionMetaData != null) {
                return partitionMetaData.leader();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static List<Integer> getPartitionIdList(String host, int port, String topic) {
        List<Integer> partitionIdList = new ArrayList<>();
        String clientName = "Client_Leader_LookUp";
        SimpleConsumer consumer;
        try {
            consumer = new SimpleConsumer(host, port, 10000, 64 * 1024, clientName);
            List<String> topics = new ArrayList<>();
            topics.add(topic);
            TopicMetadataRequest request = new TopicMetadataRequest(topics);
            TopicMetadataResponse response = consumer.send(request);

            List<TopicMetadata> topicMetadataList = response.topicsMetadata();
            System.out.println("topicMetadataList.size=" + topicMetadataList.size());
            for (TopicMetadata topicMetadata : topicMetadataList) {
                for (PartitionMetadata metadata : topicMetadata.partitionsMetadata()) {
                    System.out.println(metadata);
                    partitionIdList.add(metadata.partitionId());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return partitionIdList;
    }


}

