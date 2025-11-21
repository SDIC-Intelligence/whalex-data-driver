package com.meiya.whalex.db.module.stream;

import com.meiya.whalex.db.entity.TransactionManager;
import com.meiya.whalex.db.entity.stream.KafkaClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaTransactionManager implements TransactionManager<KafkaClient> {

    private KafkaClient kafkaClient;
    private Map<TopicPartition, Long>  topicPartitionToOffset;
    private Map<TopicPartition, Long> topicPartitionToOffsetCommit;

    public KafkaTransactionManager(KafkaClient kafkaClient) {
        this.kafkaClient = kafkaClient;
        topicPartitionToOffset = new ConcurrentHashMap<>();
        topicPartitionToOffsetCommit = new ConcurrentHashMap<>();
    }

    @Override
    public KafkaClient getTransactionClient() {
        return kafkaClient;
    }

    public void saveEndPoint(Map<TopicPartition, Long>  topicPartitionToOffset) {
        this.topicPartitionToOffset.putAll(topicPartitionToOffset);
    }

    public void saveEndPointCommit(Map<TopicPartition, Long>  topicPartitionToOffsetCommit) {
        this.topicPartitionToOffsetCommit.putAll(topicPartitionToOffsetCommit);
    }

    @Override
    public void rollback() throws Exception {
        KafkaConsumer consumer = kafkaClient.getConsumer();
        Set<Map.Entry<TopicPartition, Long>> entries = topicPartitionToOffset.entrySet();
        for (Map.Entry<TopicPartition, Long> entry : entries) {
            consumer.seek(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void commit() throws Exception {
        kafkaClient.getConsumer().commitSync();
        topicPartitionToOffset.clear();
        topicPartitionToOffsetCommit.clear();
    }

    @Override
    public void transactionTimeOut() throws Exception {
        rollback();
    }
}
