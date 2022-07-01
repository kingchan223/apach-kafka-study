package com.example;

/*
 * 모든 코드 출처 : https://github.com/bjpublic/apache-kafka-with-java
 */


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
//메시지 키가 추가된 프로듀서
public class SimpleProducer2 {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer2.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String messageKey = "sanbon";//메시지 값 설정
        String messageValue = "26";//메시지 값 설정

        /*new ProducerRecord<>(토픽이름, 메시지 key, 메시지 value*/
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageKey, messageValue);
        producer.send(record);
        logger.info("{}", record);
        System.out.println("record = " + record);
        producer.flush();
        producer.close();
    }
}