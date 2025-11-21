package com.meiya.whalex.interior.db.search.out;


import com.fasterxml.jackson.annotation.JsonProperty;
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
 * 数据量统计接口返回报文
 *
 * @author 黄河森
 * @date 2019/11/18
 * @project whale-cloud-platformX
 */
@ApiModel(value = "资源数据量统计响应报文")
@Data
public class DbCountResult extends BaseResult {

    @ApiModelProperty("总数量")
    private long total;

    @ApiModelProperty("异步任务序列号")
    private String seqId;

    @ApiModelProperty("异步任务开始时间")
    private Long executeStartTime;

    @ApiModelProperty("异步任务结束时间")
    private Long executeEndTime;
    
    private List<DbCountInfo> rows;

    public DbCountResult() {
    }

    public DbCountResult(int returnCode, String errorMsg) {
        super(returnCode, errorMsg);
    }

    public DbCountResult(ReturnCodeEnum returnCodeEnum) {
        super(returnCodeEnum);
    }

    public DbCountResult(long total, String seqId, List<DbCountInfo> rows) {
        this.total = total;
        this.seqId = seqId;
        this.rows = rows;
    }

    public DbCountResult(int returnCode, String errorMsg, long total, String seqId, List<DbCountInfo> rows) {
        super(returnCode, errorMsg);
        this.total = total;
        this.seqId = seqId;
        this.rows = rows;
    }

    public DbCountResult(ReturnCodeEnum returnCodeEnum, long total, String seqId, List<DbCountInfo> rows) {
        super(returnCodeEnum);
        this.total = total;
        this.seqId = seqId;
        this.rows = rows;
    }

    @ApiModel("组件统计详情")
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DbCountInfo {

        @ApiModelProperty("统计操作是否成功")
        private Boolean success = true;

        @ApiModelProperty("状态码")
        private int code = 0;

        @ApiModelProperty("消息")
        private String message = "成功";

        @ApiModelProperty("当前组件数据量")
        private long total;

        @ApiModelProperty("数据库表标识符")
        private String dbId;

        @ApiModelProperty("组件类型")
        private String dbType;

        @ApiModelProperty("组件类型名称")
        private String dbTypeName;

        @JsonProperty
        @ApiModelProperty(value = "数据量附加信息结果")
        private List data;
    }

}
