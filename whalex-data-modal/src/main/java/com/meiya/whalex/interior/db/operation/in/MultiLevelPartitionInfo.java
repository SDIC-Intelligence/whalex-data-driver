package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.db.constant.PartitionType;
import io.swagger.annotations.ApiModel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@ApiModel(value = "多级分区信息")
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MultiLevelPartitionInfo {

    private List<PartitionInfo> partitions;

    private PartitionType type;

    private List<PartitionEq> eq;

    @Data
    @ApiModel(value = "分区信息")
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PartitionInfo {
        private String partitionField;

        private String partitionFormat;
    }

    @Data
    @ApiModel(value = "多级eq分区")
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PartitionEq {
        private String path;
        private List<String> value;
    }
}
