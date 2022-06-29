package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
* 모든 코드 출처 : https://github.com/bjpublic/apache-kafka-with-java
*/
import java.util.Properties;

public class SimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test";//프로듀서는 전송하고자하는 토픽을 알고 있어야 한다. 토픽이름은 Producer Record 인스턴스를 생성할 때 사용
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";//전송하고자 하는 카프카 클러스터 서버의 host와 IP를 지정

    public static void main(String[] args) {
        Properties configs = new Properties();//Properties에 KafkaProducer 인스턴스를 생성하기 위한 프로듀서 옵션들을 key/value 값으로 선언. 필수 옵션을 반드시 선언해야 한다.
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);//전송하고자 하는 카프카 클러스터 서버의 host와 IP를 지정
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//메시지 키, 값을 직렬화하기 위한 직렬화 클래스 선언
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//String 객체를 전송하기 위해 String을 직렬화하는 클래스인 StringSerializer를 사용

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);//producer인스턴스는 ProducerRecord를 전송할 때 사용

        String messageValue = "testMessage";//메시지 값 설정
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        //카프카 브로커로 데이터를 보내기 위해 ProducerRecord를 생성
        //ProducerRecord는 생성자를 여러 개 가지는데, 생성자 개수에 따라 오버로딘 되어 생성됨
        //키를 따로 설정하지 않아서 키는 null로 설정되어 전송됨
        //ProducerRecord를 생성할 때 두 개의 제네릭 값. 이 값은 메시지의 키와 메시지 값의 타입을 뜻함 메시지 키와 값의 타입은 직렬화 클래스와 동일하게 설정
        producer.send(record);//send()메서드는 즉각적으로 데이터 전송 No. 파라미터로 들어간 record를 프로듀서 내부에 가지고 있다가 배치 형태로 묶어서 전송. = 배치전송
        logger.info("{}", record);
        producer.flush();//flush()를 통해 프로듀서 내부 버퍼에 가지고 있던 레코드 배치를 브로커로 전송
        producer.close();//애플리케이션을 종료하기 전에 close()메서드를 호출하여 producer 인스턴스의 리소스들을 안전하게 종료
    }
}