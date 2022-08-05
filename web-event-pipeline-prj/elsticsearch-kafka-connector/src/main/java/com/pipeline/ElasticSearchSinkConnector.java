package com.pipeline;

import com.pipeline.config.ElasticSearchSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/*
* ElasticSearchSinkConnector 클래스는 커넥터를 생성했을 때 최초로 실행된다.
* 직접적으로 데이터를 적재하는 로직을 포함하는 것은 아니고, 태스크를 실행하기 위한 이전 단계로써 설정값을 확인하고 태스크 클래스를 지정하는 역할을 한다.
*
* :: 커넥터는 그 자체로 어플리케이션 단위로 실행되는 것이 아니다. 커넥트를 통해 플러그인으로 추가하여 실행하는 것임.
* */
public class ElasticSearchSinkConnector extends SinkConnector {

    private final Logger logger = LoggerFactory.getLogger(ElasticSearchSinkConnector.class);

    private Map<String, String> configProperties;

    @Override
    public String version() {//커넥터 버전 설정. 버전은 커넥터 버전에 따른 변경사항을 확인하기 위해 버저닝(versioning)을 할 때 필요.
        return "1.0";// 지속적인 업그레이드는 고려하지 않으므로 1.0으로 설정
    }

    @Override
    public void start(Map<String, String> props) {//커넥터가 최초로 실행될 때 실행되는 부분
        this.configProperties = props;//사용자로부터 설정값을 가져와사.
        try {
            new ElasticSearchSinkConnectorConfig(props);//ElasticSearchSinkConnectorConfig 인스턴스 생성
        } catch (ConfigException e) {
            throw new ConnectException(e.getMessage(), e);//정상적이지 않은 경우 ConfigException 을 발생
        }
    }

    // 커넥터를 실행했을 경우 태스크 역할을 할 클래스를 선언.
    // 다수의 태스크를 운영할 경우 태스크 클래스 분기 로직을 넣을 수 있음
    @Override
    public Class<? extends Task> taskClass() {
        return ElasticSearchSinkTask.class;
    }

    @Override// 태스크별로 다른 설정값을 부여할 경우에는 여기에 로직을 넣을 수 있음 여기서는 모든 태스크에 동일한 설정값을 설정
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>(this.configProperties);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    //ElasticSearchSinkConnectorConfig에서 설정한 설정값을 리턴한다.
    //리턴한 설정값은 사용자가 커넥터를 생성할 때 설정값을 정상적으로 입력했는지 검증할 때 사용된다.
    @Override
    public ConfigDef config() {
        return ElasticSearchSinkConnectorConfig.CONFIG;
    }

    @Override
    public void stop() {//커넥터가 종료될 때 로그를 남긴다.
        logger.info("Stop elasticsearch connector");
    }
}