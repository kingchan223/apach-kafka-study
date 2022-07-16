package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilter {
    private static String APPLICATION_NAME = "streams-filter-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // stream_log 토픽을 가져오기 위한 소스 프로세서 작성.
        // stream()메서드로 stream_log 토픽의 데이터를 KStream 인스턴스로 만들었다.
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);

        // 데이터를 필터링하는 filter()메서드는 자바의 함수형 인터페이스인 Predicate 를 파라미타로 받는다.
        // Predicate 는 함수형 인터페이스로 특정 조건을 표현할 때 사용할 수 있는데,
        // 여기서는 메시지 key 와 메시지 value 에 대한 조건을 나타낸다. 즉 메시지 값의 길이가 5보다 큰 경우에만 필터링하도록 했다.
        KStream<String, String> filteredStream = streamLog.filter((k, v) -> v.length() > 5);

        // 필터링된 KStream 을 stream_log_filter 토픽에 저장하도록 소스 프로세서를 작성
        filteredStream.to(STREAM_LOG_FILTER);
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
