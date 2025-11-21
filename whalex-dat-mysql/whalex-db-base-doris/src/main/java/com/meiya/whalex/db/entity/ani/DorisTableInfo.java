package com.meiya.whalex.db.entity.ani;

import lombok.Data;

import java.util.Map;

/**
 * MySql 表配置信息
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Data
public class DorisTableInfo extends BaseMySqlTableInfo {

    public static final String GROUP_COMMIT_ASYNC_MODE = "async_mode";
    public static final String GROUP_COMMIT_SYNC_MODE = "sync_mode";
    public static final String GROUP_COMMIT_OFF_MODE = "off_mode";


    /**
     * 分桶数
     */
    private Integer bucketsNum;
    /**
     * 副本数
     */
    private Integer replicationNum;

    /**
     * 设置存储介质
     */
    private String storageMedium;

    /**
     * 存储到期时间
     */
    private String storageCoolDownTime;

    /**
     * 使用 colocation Join 功能时，设置这个参数为 group1
     */
    private String colocateWith;

    /**
     * 指定添加 Bloom Filter 索引的列名称列表
     */
    private String bloomFilterColumns;

    /**
     * 压缩类型
     */
    private String compression;

    /**
     * 指定序列列，只能是 date/datatime
     */
    private String sequenceCol;

    /**
     * 创建隐藏的序列类型 Date/Datatime
     */
    private String sequenceType;

    /**
     * 是否使用 light schema change 优化
     * 2.0.0 之后默认开启
     */
    private Boolean lightSchemaChange;

    /**
     * 是否禁用这个表自动 compaction
     */
    private Boolean disableAutoCompaction;

    /**
     * 是否开启单副本 compaction
     */
    private Boolean enableSingleReplicaCompaction;

    /**
     * 设置为 true 时，如果创建表没有指定 Unique、Aggregate或者Duplicate时，默认创建一个没有排序列和前缀索引的Duplicate模型表
     */
    private Boolean enableDuplicateWithoutKeysByDefault;

    /**
     * 导入数据时不写索引
     */
    private Boolean skipWriteIndexOnLoad;

    /**
     * 开启动态分区
     */
    private Boolean dynamicPartitionEnable;

    /**
     * 分区添加的时间单位
     */
    private String dynamicPartitionTimeUnit;

    /**
     * 指定向前删除的多少个分区，值需要小于0
     */
    private Integer dynamicPartitionStart;

    /**
     * 指定提前创建的分区个数
     */
    private Integer dynamicPartitionEnd;

    /**
     * 指定分区前缀
     */
    private String dynamicPartitionPrefix;

    /**
     * 分区分桶数量
     */
    private Integer dynamicPartitionBuckets;

    /**
     * 是否创建历史分区
     */
    private Boolean dynamicPartitionCreateHistoryPartition;

    /**
     * 历史分区数量
     */
    private Integer dynamicPartitionHistoryPartitionNum;

    /**
     * 指定保留的历史分区时间段
     */
    private String dynamicPartitionReservedHistoryPeriods;

    /**
     * doris 2.1 版本之后的提交模式
     * 异步：async_mode
     * 同步：sync_mode
     * 关闭：off_mode
     */
    private String groupCommit;
}
