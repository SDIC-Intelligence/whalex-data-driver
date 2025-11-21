package com.meiya.whalex.db.entity.table.partition;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * @author 黄河森
 * @date 2024/2/2
 * @package com.meiya.whalex.db.entity.table.partition
 * @project whalex-data-driver
 * @description TablePartitions
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TablePartitions {

    /**
     * 分区名称，非必填
     */
    private String partitionName;

    private List<TablePartition> partitions;

    /**
     * 扩展信息
     * 如：Hive 外部表分区存储位置
     */
    private Map<String, Object> extendInfos;

    /**
     * 分区类型：PartitionType.java
     */
    private String partitionType;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TablePartition {
        private String partitionKey;
        private Object partitionValue;
    }

}
