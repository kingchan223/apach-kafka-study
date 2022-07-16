package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
// 코 파티셔닝되어 있지 않은 데이터를 조인하는 방법 - GlobalKTable을 선언하여 사용.
import java.util.Properties;

public class KstreamGlobalKtableJoin {
    private static String APPLICATION_NAME = "global-table-join-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String ADDRESS_GLOBAL_TABLE = "address_v2";//2개의 파티션으로 이루러진 address_v2.
    private static String ORDER_STREAM = "order";//3개의 파티션으로 이루어진 order 토픽을 미리 정의.
    private static String ORDER_JOIN_STREAM = "order_join";//조인된 데이터를 저장하는 order_join 토픽

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        //globalTable()은 토픽을 GlobalKTable 인스턴스로 만드는 소스 프로세서.
        // order 토픽은 KStream 으로 사용하기 위해 stream()메서드로 KStream 인스턴스를 생성한다.
        GlobalKTable<String, String> addressGlobalTable = builder.globalTable(ADDRESS_GLOBAL_TABLE);
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM);

        // join 을 위해 KStream 인스턴스에 정의되어 있는 join() 메서드를 사용한다.
        orderStream
                .join(
                        addressGlobalTable,
                        (orderKey, orderValue) -> orderKey,//GlobalKTable은 레코드 매칭시 KStream의 메시지 key, value를 모두 사용 가능 여기서는 KStream의 메시지 키를 GlobalKTable의 메시지 키와 매칭하도록 설정.
                        (order, address) -> order + " send to " + address//주문한 물품과 주소를 조합하여 String 타입 데이터를 만들어내도록 했다.
                )
                .to(ORDER_JOIN_STREAM);// 조인을 통해 생성된 데이터를 order_join 토픽에 저장하기 위해 to() 싱크 프로세서를 사용했다.

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
