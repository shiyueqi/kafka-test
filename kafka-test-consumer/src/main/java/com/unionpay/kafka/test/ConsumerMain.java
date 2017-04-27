package com.unionpay.kafka.test;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * date: 2017/04/21 15:11.
 * author: Yueqi Shi
 */
public class ConsumerMain {
    public static final String KAFKA_TOPIC = "topic_1";

    private static final String ZOOKEEPER_ADDRESS = "172.18.55.21:2181,172.18.55.21:2182";

    private static final int THREADS_NUM = 1;

    private static final String CLIENT_ID = "consumer_test_id";

    private final ConsumerConnector consumerConnector;

    private ExecutorService executor;

    public ConsumerMain() {
        consumerConnector = Consumer.createJavaConsumerConnector(buildProperties());
        executor = Executors.newFixedThreadPool(THREADS_NUM);
    }

    /**
     * 构造连接参数
     * @return
     */
    private ConsumerConfig buildProperties() {
        Properties props = new Properties();
        props.put("zookeeper.connect", ZOOKEEPER_ADDRESS);
        props.put("group.id", "test_group");

        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        props.put("client.id", CLIENT_ID);
        return new ConsumerConfig(props);
    }

    /**
     * 开始消费
     */
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(KAFKA_TOPIC, THREADS_NUM);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(KAFKA_TOPIC);

        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerExecutorService(stream));
        }
    }

    /**
     * shutdown
     */
    public void shutdown() {
        if (consumerConnector != null) consumerConnector.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }


    public static void main(String[] args) {
        new ConsumerMain().run();
    }
}
