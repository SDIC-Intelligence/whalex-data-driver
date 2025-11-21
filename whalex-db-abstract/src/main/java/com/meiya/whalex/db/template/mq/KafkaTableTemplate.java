package com.meiya.whalex.db.template.mq;

import com.meiya.whalex.util.UUIDGenerator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author 黄河森
 * @date 2020/12/22
 * @project whalex-data-driver
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class KafkaTableTemplate {

    /**
     * 当前消费topic指定分区
     */
    private List<Integer> partitions;

    /**
     * 分区数量
     */
    private Integer numPartitions= 3;
    /**
     * 副本数量
     */
    private Integer replicationFactor = 3;

    /**
     * 存储时间 天
     */
    private Integer retentionTime;

    /**
     * 单个topic创建多个连接对象时使用
     */
    private String multipleConnect;

    //'gzip', 'snappy', lz4
    private String compressionType;

    private String deleteRetentionMs;

    private String fileDeleteDelayMs;

    private String flushMessages;

    private String flushMs;

    private String followerReplicationThrottled;

    private String indexIntervalBytes;

    private String leaderReplicationThrottledReplicas;

    private String maxMessageBytes;

    private String messageDownconversionEnable;

    private String messageFormatVersion;

    private String messageTimestampDifferenceMaxMs;

    //`CreateTime` or `LogAppendTime`
    private String messageTimestampType;

    private String minCleanableDirtyRatio;

    private String minCompactionLagMs;

    private String minInsyncReplicas;

    private String preallocate;

    private String retentionBytes;

    private String retentionMs;

    private String segmentBytes;

    private String segmentIndexBytes;

    private String segmentJitterMs;

    private String segmentMs;

    private String uncleanLeaderElectionEnable;

    @Builder
    public KafkaTableTemplate(List<Integer> partitions, Integer numPartitions, Integer replicationFactor, Integer retentionTime, String compressionType, String deleteRetentionMs, String fileDeleteDelayMs, String flushMessages, String flushMs, String followerReplicationThrottled, String indexIntervalBytes, String leaderReplicationThrottledReplicas, String maxMessageBytes, String messageDownconversionEnable, String messageFormatVersion, String messageTimestampDifferenceMaxMs, String messageTimestampType, String minCleanableDirtyRatio, String minCompactionLagMs, String minInsyncReplicas, String preallocate, String retentionBytes, String retentionMs, String segmentBytes, String segmentIndexBytes, String segmentJitterMs, String segmentMs, String uncleanLeaderElectionEnable, Boolean isMultiple) {
        this.partitions = partitions;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        this.retentionTime = retentionTime;
        this.compressionType = compressionType;
        this.deleteRetentionMs = deleteRetentionMs;
        this.fileDeleteDelayMs = fileDeleteDelayMs;
        this.flushMessages = flushMessages;
        this.flushMs = flushMs;
        this.followerReplicationThrottled = followerReplicationThrottled;
        this.indexIntervalBytes = indexIntervalBytes;
        this.leaderReplicationThrottledReplicas = leaderReplicationThrottledReplicas;
        this.maxMessageBytes = maxMessageBytes;
        this.messageDownconversionEnable = messageDownconversionEnable;
        this.messageFormatVersion = messageFormatVersion;
        this.messageTimestampDifferenceMaxMs = messageTimestampDifferenceMaxMs;
        this.messageTimestampType = messageTimestampType;
        this.minCleanableDirtyRatio = minCleanableDirtyRatio;
        this.minCompactionLagMs = minCompactionLagMs;
        this.minInsyncReplicas = minInsyncReplicas;
        this.preallocate = preallocate;
        this.retentionBytes = retentionBytes;
        this.retentionMs = retentionMs;
        this.segmentBytes = segmentBytes;
        this.segmentIndexBytes = segmentIndexBytes;
        this.segmentJitterMs = segmentJitterMs;
        this.segmentMs = segmentMs;
        this.uncleanLeaderElectionEnable = uncleanLeaderElectionEnable;
        if (isMultiple != null && isMultiple) {
            this.multipleConnect = UUIDGenerator.shortGenerate();
        }
    }
}
