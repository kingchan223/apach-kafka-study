package com.example;

import com.example.rebalanceListener.RebalanceListener;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
/*
 * 모든 코드 출처 : https://github.com/bjpublic/apache-kafka-with-java
 */

/* Consumer의 안전한 종료 */
public class SimpleConsumer8 {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer8.class);
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
        consumer.subscribe(List.of(TOPIC_NAME), new RebalanceListener());
        try{
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
        } catch(WakeupException e){ /* wakeup메서드가 호출되고 난 뒤에 poll()메서드가 호출되면 해당 catch문에 걸리게 된다.*/
            logger.warn("Wakeup consumer");
            //리소스 종료 처리
        } finally{
            consumer.close();
            // close() 메서드를 호출하면 해당 컨슈머는 더는 동작하지 않는다는 것을 알려주므로
            // 컨슈머 그룹에서 이탈되고 나머지 컨슈머들이 파티션을 할당받게 된다.
        }
    }
}
