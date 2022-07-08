package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
/*
 * 모든 코드 출처 : https://github.com/bjpublic/apache-kafka-with-java
 */

/* SimpleConsumer7의 파티션 할당 컨슈머에 할당된 토픽과 파티션에 대한 정보 확인하기 */
// 컨슈머에 할당된 토픽과 파티션에 대한 정보는 assignment() 메서드는 Set<TopicPartition> 인스턴스를 반환한다.
// TopicPartition 클래스는 토픽 이름과 파티션 번호가 포함된 객체이다.
public class SimpleConsumer8 {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer8.class);
    private final static String TOPIC_NAME = "test";
    private final static int PARTITION_NUMBER = 0; /* PARTITION_NUMBER 추가 */
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
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));
        Set<TopicPartition> assignedTopicPartition = consumer.assignment(); /*assignment() 메서드는 Set<TopicPartition> 인스턴스를 반환*/
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            HashMap<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
            for (ConsumerRecord<String, String> record : records){
                logger.info("{}",record);
                currentOffset.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, null)
                );
                consumer.commitSync(currentOffset);
            }
        }
    }
}
