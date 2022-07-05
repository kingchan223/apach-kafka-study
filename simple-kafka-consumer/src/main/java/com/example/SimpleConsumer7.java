package com.example;

import com.example.rebalanceListener.RebalanceListener;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
/*
 * 모든 코드 출처 : https://github.com/bjpublic/apache-kafka-with-java
 */

/* 파티션 할당 컨슈머*/
// 컨슈머 운영시 subscribe() 메서드를 사용하여 구독 형태로 사용하는 것 외에 직접 파티션을 컨슈머에 명시적으로 할당하여 운영할 수도 있음
// 컨슈머가 어떤 토픽, 파티션을 할당할지를 명시적으로 선언할 때는 assign()메서드를 사용.
// assign() - TopicPartition Collection 타입을 파라미터로 받음. TopicPartition 객체는 카프카 라이브러리 내/외부에서 사용되는 토픽, 파티션의 정보를 담는 객체로 사용됨.
public class SimpleConsumer7 {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer7.class);
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
        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));//subscribe 대신 assign 사용
        // test의 0번 파티션을 할당하여 레코드들을 가져온다. subscribe()메서드를 사용할 떄와 다르게 직접 컨슈머가 특정 토픽, 특정 파티션에 할당되므로 리밸러싱하는 과정이 없어짐
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
