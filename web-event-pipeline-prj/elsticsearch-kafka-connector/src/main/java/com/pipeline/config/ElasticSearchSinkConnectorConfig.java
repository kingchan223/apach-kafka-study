package com.pipeline.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.*;

import java.util.Map;

public class ElasticSearchSinkConnectorConfig extends AbstractConfig {

    public static final String ES_CLUSTER_HOST = "es.host";//elasticsearch host 이름
    private static final String ES_CLUSTER_HOST_DEFAULT_VALUE = "localhost";
    private static final String ES_CLUSTER_HOST_DOC = "엘라스틱서치 호스트를 입력";

    public static final String ES_CLUSTER_PORT = "es.port";//elasticsearch Port 이름
    private static final String ES_CLUSTER_PORT_DEFAULT_VALUE = "9200";
    private static final String ES_CLUSTER_PORT_DOC = "엘라스틱서치 포트를 입력";

    public static final String ES_INDEX = "es.index";//데이터의 인덱스 이름
    private static final String ES_INDEX_DEFAULT_VALUE = "kafka-connector-index";
    private static final String ES_INDEX_DOC = "엘라스틱서치 인덱스를 입력";

    // 위에서 선언한 3개의 설정값(es.host, es.port, es.index)을 ConfigDef 클래스로 생성
    // ConfigDef 인스턴스는 커넥터에서 설정값이 정상적으로 들어왔는지 검증하기 위해 사용된다.
    public static ConfigDef CONFIG = new ConfigDef()
            .define(ES_CLUSTER_HOST, Type.STRING, ES_CLUSTER_HOST_DEFAULT_VALUE, Importance.HIGH, ES_CLUSTER_HOST_DOC)
            .define(ES_CLUSTER_PORT, Type.INT, ES_CLUSTER_PORT_DEFAULT_VALUE, Importance.HIGH, ES_CLUSTER_PORT_DOC)
            .define(ES_INDEX, Type.STRING, ES_INDEX_DEFAULT_VALUE, Importance.HIGH, ES_INDEX_DOC);

    public ElasticSearchSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}