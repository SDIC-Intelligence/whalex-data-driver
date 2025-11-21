package com.meiya.whalex.db.kafka.admin.client;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

/**
 * @author 黄河森
 * @date 2021/9/20
 * @package com.meiya.whalex.db.kafka.admin.client
 * @project whalex-data-driver
 */
@InterfaceStability.Evolving
public class MyListConsumerGroupOffsetsResult {

    final KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> future;

    public MyListConsumerGroupOffsetsResult(KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
        this.future = future;
    }

    public KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> partitionsToOffsetAndMetadata() {
        return this.future;
    }

}
