package com.likg.kafka;

import com.alibaba.fastjson.JSONObject;
import com.sankuai.xm.proto.constant.DeviceType;
import com.sankuai.xm.proto.constant.IMMsgType;
import com.sankuai.xm.proto.im.PIMSendMsg;
import com.sankuai.xm.proto.im.PIMTextInfo;
import com.sankuai.xm.proto.im.ProtoIMIds;
import com.sankuai.xm.protobase.ProtoAppid;
import com.sankuai.xm.protobase.ProtoPacket;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 *
 */
public class ProducerDemo2 {

    public static void main(String[] args) throws InterruptedException {
        Random rnd = new Random();
        int events = 1;

        // 设置配置属性
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.1.147:9092");
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
        for (long i = 0; i < events; i++) {
            System.out.println("i==========================" + i);
            long runtime = new Date().getTime();
            String ip = "kk" + i;//rnd.nextInt(255);
            String msg = System.currentTimeMillis() + "";
            msg = "123-" + msg;


            int fromUserId = 74;
            int toUserId = 136;
            PIMSendMsg pimSendMsg = new PIMSendMsg();
            pimSendMsg.setCts(System.currentTimeMillis());
            pimSendMsg.setMsgId(Math.abs(new Random().nextLong()));
            pimSendMsg.setMsgUuid(pimSendMsg.getMsgId() + "_uuid");
            pimSendMsg.setFromName("");
            pimSendMsg.setFromUid(fromUserId);
            pimSendMsg.setAppId(ProtoAppid.APPID_XMAPP);
            pimSendMsg.setToAppId(ProtoAppid.APPID_XMAPP);
            pimSendMsg.setDeviceType(DeviceType.APP);
            pimSendMsg.setUri(ProtoIMIds.URI_IM_SEND_MSG);

            JSONObject jsonObject = new JSONObject();

            msg = 1 + "-" + fromUserId + "----" + toUserId + "--";
            jsonObject.put("content", msg);
            jsonObject.put("extra", "1111");
            ProtoPacket packet = parsePIMTextInfo(jsonObject);
            pimSendMsg.setType(IMMsgType.IM_MSG_TYPE_TEXT);

            pimSendMsg.setMessage(packet.getPacketBytes());

            pimSendMsg.setToUid(toUserId);
            byte[] buf = pimSendMsg.marshall();
            //System.out.println("buf size===" + buf.length);


            //buf = "abc".getBytes();

            Object msgObj = buf;

            //如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0
            KeyedMessage data = new KeyedMessage("xm-msgbox", ip, "bbbbbbb");
            producer.send(data);
            //Thread.sleep(1000);
        }
        System.out.println("耗时:" + (System.currentTimeMillis() - start));
        // 关闭producer
        producer.close();
    }

    private static PIMTextInfo parsePIMTextInfo(JSONObject jsonObject) {
        PIMTextInfo pimTextInfo = new PIMTextInfo();
        String content = jsonObject.getString("content");
        String extra = jsonObject.getString("extra");
        pimTextInfo.font_name = jsonObject.getString("font_name");
        pimTextInfo.font_size = jsonObject.getIntValue("font_size");
        pimTextInfo.bold = jsonObject.getBooleanValue("bold");
        pimTextInfo.cipher_type = jsonObject.getShortValue("cipher_type");
        pimTextInfo.text = content;
        pimTextInfo.extra = extra;
        return pimTextInfo;
    }
}