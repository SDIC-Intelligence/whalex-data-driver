package com.meiya.whalex.interior.db.search.out;


import com.meiya.whalex.interior.base.BaseResult;
import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 小文件合并响应报文
 *
 * @author 黄河森
 * @date 2019/11/18
 * @project whale-cloud-platformX
 */
@ApiModel(value = "分布式文件系统小文件合并相应报文")
@Data
public class FileSystemMergeDataResult extends BaseResult {

    @ApiModelProperty("异步任务序列号")
    private String seqId;

    private List<MergeData> data;

    public FileSystemMergeDataResult() {
    }

    public FileSystemMergeDataResult(int returnCode, String errorMsg) {
        super(returnCode, errorMsg);
    }

    public FileSystemMergeDataResult(ReturnCodeEnum returnCodeEnum) {
        super(returnCodeEnum);
    }

    public FileSystemMergeDataResult(String seqId, List<MergeData> data) {
        this.seqId = seqId;
        this.data = data;
    }

    public FileSystemMergeDataResult(int returnCode, String errorMsg, String seqId, List<MergeData> data) {
        super(returnCode, errorMsg);
        this.seqId = seqId;
        this.data = data;
    }

    public FileSystemMergeDataResult(ReturnCodeEnum returnCodeEnum, String seqId, List<MergeData> data) {
        super(returnCodeEnum);
        this.seqId = seqId;
        this.data = data;
    }

    @ApiModel("组件统计详情")
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MergeData {

        /**
         * 上级目录
         */
        @ApiModelProperty(value = "上级目录")
        private String parentPath;

        /**
         * 被合并的目标小文件
         */
        @ApiModelProperty(value = "被合并的目标小文件")
        private List<String> mergeTargetFiles;

        /**
         * 合并生成的文件
         */
        @ApiModelProperty(value = "合并生成的文件")
        private List<String> mergeGenerateFiles;
    }

}
