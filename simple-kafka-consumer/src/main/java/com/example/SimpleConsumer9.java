package com.example;

import com.example.rebalanceListener.RebalanceListener;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
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

/* Consumer의 안전한 종료 + 셧다운 훅 스레드가 wakeup메서드를 호출하게 하기*/
// 자바 애플리케이션의 경우 내부 코드에서 셧다운 훅(shutdownhook)을 구현하여 안전한 종료를 명시작으로 구현할 수 있다.
// * 셧다운훅 : 사용자 또는 운영체제로부터 종료 요청을 받으면 실행되는 스레드.
// 아래 코드는 셧다운 훅을 사용하여 wakeup()메서드를 호출하는 예제코드.
public class SimpleConsumer9 {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer9.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutDownThread());
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
    static class ShutDownThread extends Thread{
        public void run() {
            logger.info("Shutdown hook");
            //consumer.wakeup();
        }
    }
    /*
    * 사용자는 안전한 종료를 위해 위 코드로 실행된 애플리케이션에 kill -TERM {프로세스 번호}를 호출하여 셧다운 훅을 발생시킬 수 있다.
    * 셧다운 훅이 발생하면 사용자가 정의한 ShutdownThread 스레드가 실행되면서 wakeup()메서드가 호출되어 컨슈머를 안전하게 종료한다.
    * */
}

