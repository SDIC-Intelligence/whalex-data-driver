package com.meiya.whalex.interior.db.operation.in;

import io.swagger.annotations.ApiModel;
import lombok.Data;

import java.util.List;

@ApiModel(value = "合并数据条件")
@Data
public class MergeDataParamCondition {

    /**
     * 源路径
     */
    private String src;

    /**
     * 需要合并的文件大小
     */
    private long maxMergeSize = 10 * 1024 * 1024;

    /**
     * 合并的块大小
     */
    private Long blockSize;

    /**
     * 排除的文件
     */
    private List<String> excludeFiles;

    /**
     * 排除的目录
     */
    private List<String> excludePaths;

    /**
     * 执行的合并周期值，作用于分区文件目录类型
     */
    private Integer exePeriodValue;

    /**
     * 小文件占比
     */
    private Double mergeScale;
}
