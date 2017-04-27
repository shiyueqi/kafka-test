package com.unionpay.kafka.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * date: 2017/04/21 14:54.
 * author: Yueqi Shi
 */
public class ProducerMain {
    public  static final String KAFKA_TOPIC = "topic_1";

    private static final String BROKERS_ADDRESS = "172.18.55.21:9092,172.18.55.21:9093";

    private static final int REQUEST_REQUIRED_ACKS = 1;

    public static final String MESSAGE = "message";

    private static final String CLIENT_ID = "producer_test_id";

    private KafkaProducer<String, String> producer;

    public ProducerMain() {
        producer = new KafkaProducer<String, String>(buildProperties());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.print("Kafka producer graceful shutdown.");
                ProducerMain.this.shuntown();
            }
        });
    }

    private Properties buildProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS_ADDRESS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, String.valueOf(REQUEST_REQUIRED_ACKS));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);

        return props;
    }

    /**
     * 开始生产msg
     */
    public void run() {
        for (int i = 0; i < 10000; i++) {
            String message = ProducerMain.MESSAGE + "_" + i;

            try {
                producer.send(new ProducerRecord<String, String>(ProducerMain.KAFKA_TOPIC, message)).get();

                //Thread.sleep(1000);
                System.out.println("produce: " + message + " success.");
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("produce: " + message + " fail.");
            } catch (ExecutionException e) {
                e.printStackTrace();
                System.out.println("produce: " + message + " fail.");
            }
        }
    }

    /**
     * shuntown
     */
    public void shuntown() {
        if (null != producer) {
            producer.close();
            producer = null;
        }
    }

    public static void main(String[] args) {
        new ProducerMain().run();
    }
}
