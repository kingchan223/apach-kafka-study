package com.example;

/*
* 모든 코드 출처 : https://github.com/bjpublic/apache-kafka-with-java
*/

import com.example.partitioner.CustomPartitioner;
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
public class SimpleProducer5 {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer5.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
        String messageValue = "simpleProducer5";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);

        RecordMetadata metadata = producer.send(record).get();//get 메서드를 사용하면 프로듀서로 보낸 데이터의결과를 동기작ㅇ로 가져올 수 있다.
        logger.info("{}", metadata.toString()); // 출력결과: [main] INFO com.example.SimpleProducer5 - test-2@1
        // test 토픽의 2번 파티션에 적재되었으며 해당 레코드에 부여된 오프셋 번호는 1번
        // 이렇게 동기로 프로듀서의 전송 결과를 확인하는 것은 빠른 전송에 방해가 된다.<-(프로듀서가 전송하고 난 뒤 브로커로부터 전송에 대한 응답 값을 받기 전까지 대기하기 때문)
        // 해서 비동기로 결과를 확인할 수 있도록 Callback 인터페이스를 제공한다. 사용자는 사용자 정의 Callback 클래스를 생성하여 레코드의 전송 결과에 대응하는 로직을 만들 수 있음
        // 이를 ProducerCallback 이라는 이름의 클래스로 만들고, SimpleProducer6에 적용해보자.
        producer.flush();
        producer.close();
    }
}