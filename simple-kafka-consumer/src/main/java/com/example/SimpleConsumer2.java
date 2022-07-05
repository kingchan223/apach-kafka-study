package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
/*
 * 모든 코드 출처 : https://github.com/bjpublic/apache-kafka-with-java
 */

// 동기 오프셋 커밋
// 데이터 중복이나 유실을 허용하지 않는 서비스라면 자동 커밋을 해서는 안된다. 해서 명시적으로 오프셋을 커밋하기 위해 poll()메서드를 호출 이후에 반환받은
// 데이터 처리가 완료되고, commitSync() 메서드를 호출하면 된다.
// commitSync() 메서드는 poll()메서드를 통해 반환된 레코드의 가장 마지막 오프셋을 기준으로 커밋을 수행한다.
// commitSync() 메서드는 브로커에 커밋 요청을 하고 커밋이 정상적으로 처리되었는지 응답하기까지 기다리는데 이는 컨슈머의 처리량에 영향을 미친다.
// 데이터 처리 시간에 비해 커밋 요청 및 응답에 시간이 오래 걸린다면 동일 시간당 데이터 처리량이 줄어들기 때문이다.
public class SimpleConsumer2 {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer2.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());//key, value 모두 반드시 프로듀서가 직렬화한 클래스로 역직렬화 해야한다.
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC_NAME));
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) logger.info("{}",record);
            //poll()메서드로 받은 가장 마지막 레코드의 오프셋을 기준으로 커밋하기에
            //poll()메서드로 받은 모든 레코드의 처리가 끝난 이후 commitSync() 메서드를 호출해야 한다.
            consumer.commitSync();/* 동기 오프셋 커밋 */
        }
    }
}
