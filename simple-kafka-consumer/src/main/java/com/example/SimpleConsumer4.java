package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
/*
 * 모든 코드 출처 : https://github.com/bjpublic/apache-kafka-with-java
 */

// 비동기 오프셋 커밋
// 동기 오프셋 커밋을 할 경우 커밋 응답을 기다리는 동안 데이터 처리가 일시적으로 중단 되어 더 많은 데이터를 처리하기 위해서 비동기 오프셋 커밋을 사용할 수 있다.
// 비동기 오프셋 커밋은 commitAsync() 메서드를 호출하여 사용할 수 있다.
public class SimpleConsumer4 {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer4.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC_NAME));
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records){
                logger.info("{}",record);
            }
            consumer.commitAsync(); /* Async()사용하여 비동기로 오프셋 커밋 */
            // 비동기 오프셋 커밋도 동기 오프셋 커밋과 마찬가지로 poll()메서드로 리턴된 가장 마지막 레코드를 기준으로 오프셋을 커밋한다.
            // 다만, 동기 오프셋 커밋과 달리 커밋이 완료될 때까지 기다리지 않는다. --> 동기식 보다 동일 시간당 데이터 처리량이 더 많음
            // 비동기 오프셋 커밋을 사용할 경우 비동기로 커밋 응답을 받기 때문에 callback 함수를 파라미터로 받아서 결과를 얻을 수 있다. <- SimpleConsumer5
        }
    }
}
