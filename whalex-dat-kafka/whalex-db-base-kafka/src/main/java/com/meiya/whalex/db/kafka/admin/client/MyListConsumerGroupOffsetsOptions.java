package com.meiya.whalex.db.kafka.admin.client;

import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.List;

/**
 * @author 黄河森
 * @date 2021/9/20
 * @package com.meiya.whalex.db.kafka.admin.client
 * @project whalex-data-driver
 */
@InterfaceStability.Evolving
public class MyListConsumerGroupOffsetsOptions extends AbstractOptions<MyListConsumerGroupOffsetsOptions> {

    private List<TopicPartition> topicPartitions = null;

    public MyListConsumerGroupOffsetsOptions() {
    }

    public MyListConsumerGroupOffsetsOptions topicPartitions(List<TopicPartition> topicPartitions) {
        this.topicPartitions = topicPartitions;
        return this;
    }

    public List<TopicPartition> topicPartitions() {
        return this.topicPartitions;
    }

}
