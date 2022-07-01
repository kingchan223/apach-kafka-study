package com.example;

/*
 * 모든 코드 출처 : https://github.com/bjpublic/apache-kafka-with-java
 */


import com.example.partitioner.CustomPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

// Custom Partitioner 를 사용하는 프로듀서
public class SimpleProducer4 {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer4.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());/* 커스텀 Partitioner 지정 */

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
        String messageKey = "sanbon";
        String messageValue = "27";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME,  messageKey, messageValue);

        producer.send(record);
        logger.info("{}", record);
        System.out.println("record = " + record);
        producer.flush();
        producer.close();
    }
}