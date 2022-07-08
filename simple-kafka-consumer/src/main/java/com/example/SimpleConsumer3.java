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

// 동기 오프셋 커밋
// + commitSync()에 파라미터가 들어가지 않으면 poll()로 반환된 가장 마지막 레코드의 오프셋을 기준으로 커밋된다.
//   하여 '개별 레코드 단위'로 매번 오프셋을 진행하고 싶다면 commitSync() 메서드에 Map<TopicPartition, OffsetAndMetadata>를 인스턴스로 넣으면 된다.
public class SimpleConsumer3 {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer3.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);/* 자동 오프셋 커밋을 false (명시적 오프셋 커밋을 위해서는 ENABLE_AUTO_COMMIT_CONFIG를 false로 해야함 */
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC_NAME));
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            HashMap<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
            for (ConsumerRecord<String, String> record : records){
                logger.info("{}",record);
                currentOffset.put(
                        new TopicPartition(record.topic(), record.partition()),//처리를 완료한 record 의 정보를 토대로 Map 인스턴스에 key, value 를 넣는다. 주의할 점은 현재 처리한 오프셋에 1을 더한 값을 커밋해야 한다는 것. 이후에 컨슈머가 poll()을 수행할 때 마지막으로 커밋한 오프셋부터 레코드를 리턴하기 때문
                        new OffsetAndMetadata(record.offset() + 1, null)
                );
                consumer.commitSync(currentOffset);
                // TopicPartition 과 OffsetAndMetadata 로 이루어진 HashMap 을 commitSync()메서드의 파라미터로 넣어
                // 호출하면 해당 특정 토픽, 파티션의 오프셋이 매번 커밋된다.
            }

        }
    }
}
