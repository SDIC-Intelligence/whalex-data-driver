package com.meiya.whalex.db.entity.stream;

import com.meiya.whalex.db.entity.AbstractDbTableInfo;
import lombok.Data;

import java.util.List;

/**
 * 数据库表配置信息
 */
@Data
public class KafkaTableInfo extends AbstractDbTableInfo {
    /**
     * 指定当前 topic 分区
     */
    private List<Integer> partitions;
    private Integer numPartitions = 3;
    private Integer replicationFactor = 3;
    private Integer retentionTime;
    /**
     * 单个topic创建多个连接对象时配置
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
}
