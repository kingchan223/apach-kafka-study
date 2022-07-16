package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

/* 스트림즈 역할을 실행하는 구문 */
public class SimpleKafkaProcessor {
    private static String APPLICATION_NAME = "processor-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new Topology();//Topology 클래스는 프로세서 API를 사용한 토폴로지를 구성하기 위해 사용한다.
        topology.addSource("Source", STREAM_LOG)//stream_log 토픽을 소스 프로세서로 가져오기 위해 addSource()메서드 사용. 첫번째에는 소스 프로세스의 이름. 두번째에는 대상 토픽의 이름입력
                .addProcessor("Process", () -> new FilterProcessor(), "Source")//스트림 프로세서 등록. 세번째 파라미터에는 부모 노드를 입력해야 한다.
                .addSink("Sink", STREAM_LOG_FILTER, "Processor");//데이터를 저장하기 위해 addSink로 싱크 프로세서 등록

        KafkaStreams streaming = new KafkaStreams(topology, props);
        streaming.start();
    }
}
