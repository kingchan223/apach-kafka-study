package com.example.rebalanceListener;


import com.example.SimpleConsumer6;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class RebalanceListener implements ConsumerRebalanceListener {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerRebalanceListener.class);

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.warn("Partitions are assigned : " + partitions.toString());
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.warn("Partitions are revoked : " + partitions.toString());
        //consumer.commitSync(currentOffset);
    }
}
