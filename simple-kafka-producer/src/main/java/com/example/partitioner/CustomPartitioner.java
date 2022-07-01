package com.example.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    // partition 메서드의 리턴값은 주어진 레코드가 들어갈 파티션 번호이다.
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        //키를 지정하지 않았다면 InvalidRecordException 에러 뱉음
        if(keyBytes == null) throw new InvalidRecordException("Need message key");

        //메시지 키가 'sanbon' 이라면 파티션 0번으로 지정
        if(((String)key).equals("sanbon")) return 0;

        // 'sanbon'이 아닌 메시지 키를 가진 레코드는 해시값을 지정하여 파티션에 매칭되도록한다.
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;

    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
