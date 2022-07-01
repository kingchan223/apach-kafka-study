package com.example;

/*
* 모든 코드 출처 : https://github.com/bjpublic/apache-kafka-with-java
*/

import com.example.callback.ProducerCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

// 브로커 정상 전송 여부를 확인하는 프로듀서
public class SimpleProducer6 {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer6.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
        String messageValue = "simpleProducer6";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        // 레코드 전송 후 비동기로 결과를 받기 위해서는 KafkaProducer 인스턴스의 send()메서드 호출 시
        // ProducerRecord 객체와 함께 사용자 정의 Callback 클래스를 넣으면 된다.
        // 비동기로 하면 더 빠른 속도로 데이터를 추가할 수 있지만 데이터의 순서가 중요한 경우에는 사용하면 안된다.
        producer.send(record, new ProducerCallback());/*ProducerCallback 추가*/
        producer.flush();
        producer.close();
    }
}