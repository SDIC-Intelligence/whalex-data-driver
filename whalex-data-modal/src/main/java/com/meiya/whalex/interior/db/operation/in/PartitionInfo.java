package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.db.constant.PartitionType;
import io.swagger.annotations.ApiModel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@ApiModel(value = "分区信息")
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PartitionInfo {

    private String partitionField;

    private String partitionFormat;

    private PartitionType type;

    private boolean autoPartition;

    private List<PartitionList> in;

    private List<PartitionRange> range;

    private List<PartitionHash> hash;

    private List<PartitionEq> eq;

    @Data
    @ApiModel(value = "hash分区")
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PartitionHash {
        private String partitionName;
    }

    @Data
    @ApiModel(value = "list分区")
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PartitionList {
        private String partitionName;
        private List<String> list;
    }

    @Data
    @ApiModel(value = "range分区")
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PartitionRange {
        private String partitionName;
        private String left;
        private String right;

    }

    @Data
    @ApiModel(value = "eq分区")
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PartitionEq {
        private String path;
        private String value;
    }
}
