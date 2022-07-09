package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
/*
 * 모든 코드 출처 : https://github.com/bjpublic/apache-kafka-with-java
 */

/* Admin API */
public class AdminClientAPI {
    private final static Logger logger = LoggerFactory.getLogger(AdminClientAPI.class);
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);//어드민API는 클러스터에 대한 정보만 설정해면 된다.
        AdminClient admin = AdminClient.create(configs);
        logger.info("== Get broker information");
        for(Node node : admin.describeCluster().nodes().get()){
            ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
            DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(cr));
            describeConfigs.all().get().forEach((broker, config)-> {
                config.entries().forEach(configEntry -> logger.info(configEntry.name() + "= " + configEntry.value()));
            });
        }

        admin.close();
        // 어드민 API를 활용할 때 클러스터 버전과 클라이언트 버전을 맞춰서 사용해야 한다.
        // 어드민 API의 많은 부분이 버전이 올라가면서 자주 바뀌기 때문
    }
}

