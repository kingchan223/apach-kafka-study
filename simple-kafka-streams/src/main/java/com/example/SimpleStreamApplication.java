package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
// stream_log topic --> stream_log_copy topic 으로 데이터를 전달하는 stream 구현하기
public class SimpleStreamApplication {

    // 스트림즈 어플리케이션은 애플리이션 ID를 지정해야 한다. 애플리케이션 아이디 값을 기준으로 병렬처리하기 때문.
    // 또 다른 스트림즈 어플리케이션을 운영하고 싶다면 기존에 작성되지 않은 어플리케이션 ID를 사용하야 한다.
    private static String APPLICATION_NAME = "streams-application";
    // 스트림즈 어플리케이션과 연동할 카프카 클러스터 정보를 입력한다. 컨슈머, 프로듀서처럼 1개 이상의 카프카 브로커 호스트와 포트 정보를 입력한다.
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_COPY = "stream_log_copy";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();//StreamsBuilder 는 스트림 토폴로지를 정의하기 위한 용도.
        // stream_log 토픽으로부터 KStream 객체를 만들기 위해 StreamBuild 의 stream() 메서드를 사용.
        // StreamBuilder 는 stream() 외에 KTable 을 만드는 table(), GlobalKTable 을 만드는 globalTable()메서드도 지원.
        // stream(), table(), globalTable() 메서드들은 최초의 토픽 데이터를 가져오는 소스 프로세서이다.
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);
        // stream_log 토픽을 담은 KStream 객체를 다른 토픽으로 전송하기 위해 to()메서드를 사용. to()메서드는 KStream 인스턴스의 데이터들을
        // 특정 토픽으로 저장하기 위한 용도로 사용된다. 즉, to()메서드는 싱크 프로세서이다.
        streamLog.to(STREAM_LOG_COPY);
        // StreamsBuilder 로 정의한 토폴로지에 대한 정보와 스트림즈 실행을 위한 기본 옵션을 파라미터로 KafkaStreams인스턴스를 생성한다.
        // kafKaStreams 인스턴스를 실행하려면 start() 메서드를 사용하면 된다. 이 스트림즈 애플리케이션은 stream_log 토픽의 데이터를 Stream_log_copy 토픽으로 전달한다.
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}