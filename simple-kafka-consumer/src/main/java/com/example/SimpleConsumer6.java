package com.example;

import com.example.rebalanceListener.RebalanceListener;
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

/* 리밸런스 리스너를 가진 컨슈머*/
// 컨슈머 그룹에서 컨슈머가 추가 또는 제거되면 파티션을 컨슈머에 재할당하는 과정인 리밸런스가 일어난다.
// poll() 메서드를 통해 반환받은 데이터를 모두 처리하기 전에 리밸런스가 발생하면 데이터를 중복처리할 수 있는 위험이 있다.
// 왜냐하면 poll() 메서드를 통해 받은 데이터 중 일부를 처리했으나 '커밋'하지 않았기 때문이다.
// 리밸런스 발생 시 데이터를 중복 처리하지 않기 위해서는 리밸런스 발생 시 처리한 데이터를 기준으로 커밋을 시도해야 한다.
// 이를 위해 카프카는 ConsumerRebalanceListener 인터페이스를 지원한다.
// ConsumerRebalanceListener:
// onPartitionAssigned()메서드와 onPartitionRevoked()메서드로 이루어짐
// onPartitionAssigned() - 리밸런스가 끝난 뒤에 파티션이 할당 완료되면 호출되는 메서드
// onPartitionRevoked()  - 리밸런스가 시작되기 직전에 호출되는 메서드 (여기에 커밋을 구현하여 처리하면 된다.)
public class SimpleConsumer6 {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer6.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);//리밸런스 발생 시 수동 커밋을 하기 위해 false로 설정
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC_NAME), new RebalanceListener());/*RebalanceListener*/
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
