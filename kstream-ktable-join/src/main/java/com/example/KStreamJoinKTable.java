package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class KStreamJoinKTable {
    private static String APPLICATION_NAME = "order-join-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String ADDRESS_TABLE = "address";
    private static String ORDER_STREAM = "order";
    private static String ORDER_JOIN_STREAM = "order_join";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // address 토픽을 KTable 로 가져올 때는 table() 메서드를 소스 프로세서로 사용하면 된다.
        KTable<String, String> addressTable = builder.table(ADDRESS_TABLE);
        // order 토픽을 KStream 으로 가져올 것이므로 stream() 메서드를 소스 프로세서로 사용한다.
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM);

        // join 을 위해 KStream 인스턴스에 정의되어 있는 join() 메서드를 사용한다.
        // 첫 번째 파라미터 : 조인을 수행할 KTable 인스턴스를 넣는다.
        // 두 번째 파라미터 : KStream 과 KTable 에서 동일한 메시지 키를 가진 데이터를 찾았을 경우 각각의 메시지 값을 조합해서 어떤 데이터를 만들지 정의한다.
        //                여기서는 order 토픽의 물품 이름과 address 토픽의 주소를 조합하여 새로운 메시지 값을 만든다.
        orderStream
                .join(
                    addressTable,
                    (order, address) -> order + " send to " + address
                )
                .to(ORDER_JOIN_STREAM);// 조인을 통해 생성된 데이터를 order_join 토픽에 저장하기 위해 to() 싱크 프로세서를 사용했다.

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
