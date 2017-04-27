package com.unionpay.kafka.test;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * date: 2017/04/24 13:53.
 * author: Yueqi Shi
 */
public class ConsumerExecutorService implements Runnable {
    private KafkaStream stream;

    public ConsumerExecutorService(KafkaStream stream) {
        this.stream = stream;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            String message = new String(it.next().message());
            System.out.println("consume: " + message);
        }
    }
}
