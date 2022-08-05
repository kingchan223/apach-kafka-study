package com.pipeline;

import com.pipeline.consumer.ConsumerWorker;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HdfsSinkApplication {
    private final static Logger logger = LoggerFactory.getLogger(HdfsSinkApplication.class);

    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";// 카프카 클러스터 정보
    private final static String TOPIC_NAME = "select-color";// 토픽 이름
    private final static String GROUP_ID = "color-hdfs-save-consumer-group";//컨슈머 그룹
    private final static int CONSUMER_COUNT = 3;//생성할 스레드 개수. 추후 파티션이 늘어날 경우 변수를 변경하고 재배포, 실행함으로써 파티션과 동일 개수의 컨슈머 스레드를 동적으로 운영 가능
    private final static List<ConsumerWorker> workers = new ArrayList<>();

    public static void main(String[] args) {
        // 컨슈머의 안전한 종료를 위해 셧다운 훅 선언.
        // 셧다운 훅 발생 -> 각 컨슈머 스레드들에 stopAndWakeup() 메서드로 종료를 알림
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 컨슈머 스레드를 스레드 풀로 관리하기 위해 newCachedThreadPool()을 생성
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < CONSUMER_COUNT; i++) {// 컨슈머 스레드를 CONSUMER_COUNT 변수 값(3)만큼 생성
            // 컨슈머 스레드 인스턴스들을 묶음으로 관리하기 위해 List<ConsumerWorker>로 선언된 workers 변수에 추가한다.
            workers.add(new ConsumerWorker(configs, TOPIC_NAME, i));
        }
        workers.forEach(executorService::execute);//execute() 메서드로 컨슈머 스레드 인스턴스들을 스레드 풀에 포함시켜 실행.
    }

    static class ShutdownThread extends Thread {
        public void run() {
            logger.info("Shutdown hook");
            workers.forEach(ConsumerWorker::stopAndWakeup);
        }
    }
}