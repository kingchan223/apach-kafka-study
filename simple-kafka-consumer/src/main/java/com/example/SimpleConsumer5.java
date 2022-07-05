package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
/*
 * 모든 코드 출처 : https://github.com/bjpublic/apache-kafka-with-java
 */

// 비동기 오프셋 커밋
// + callback 함수를 파라미터로 받아서 결과 얻기
public class SimpleConsumer5 {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer5.class);
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
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if(exception!=null) System.err.print("Commit failed");
                    else System.out.println("Commit succeeded");
                    if(exception!=null) logger.error("Commit failed for offset {}", offsets, exception);
                }
            });
            // OffsetCommitCallback 함수는 commitAsync()의 응답을 받을 수 있도록 도와주는 콜백 인터페이스이다.
            // 비동기로 받은 응답은 onComplete()메서드를 통해 확인할 수 있다.
        }
    }
}
