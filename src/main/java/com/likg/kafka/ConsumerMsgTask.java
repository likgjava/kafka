package com.likg.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 *
 */
public class ConsumerMsgTask implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;

    public ConsumerMsgTask(KafkaStream stream, int threadNumber) {
        m_threadNumber = threadNumber;
        m_stream = stream;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {

            it.next();
            //String msg = new String(it.next().message());
            //System.out.println(ConsumerDemo.count++);
            //System.out.println("msg====" + msg);
            //Long now = System.currentTimeMillis();
            //Long gap = 0L;
//            try {
//                gap = now - Long.parseLong(msg);
//            } catch (Exception e) {
//            }
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            //System.out.println("now====" + now);
            //System.out.println("Thread " + m_threadNumber + "gap=========" + gap);
        }

        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}