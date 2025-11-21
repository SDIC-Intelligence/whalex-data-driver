package com.meiya.whalex.db.template.ani;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * doris 数据库表模板
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DorisTableConfTemplate {

    private String tableName;

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
    private GroupCommitEnum groupCommit;

    public DorisTableConfTemplate(String tableName) {
        this.tableName = tableName;
    }

    public enum GroupCommitEnum {
        ASYNC_MODE("async_mode"),
        SYNC_MODE("sync_mode"),
        OFF_MODE("off_mode"),
        ;
        private final String type;

        @JsonValue
        public String getType() {
            return type;
        }

        GroupCommitEnum(String type) {
            this.type = type;
        }
    }
}
