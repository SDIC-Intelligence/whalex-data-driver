package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.LinkedList;
import java.util.List;

/**
 * 创建表请求报文
 *
 * @author 黄河森
 * @date 2019/12/3
 * @project whale-cloud-platformX
 */
@ApiModel("数据库表修改请求报文")
@Data
public class AlterTableOperation extends BaseQuery {

    @ApiModelProperty(value = "数据库表标识符")
    private String dbId;

    @ApiModelProperty(value = "表字段新增信息")
    private List<AlterTableParamCondition.AddTableFieldParam> addTableFieldParamList = new LinkedList<>();

    @ApiModelProperty(value = "修改表字段")
    private List<AlterTableParamCondition.UpdateTableFieldParam> updateTableFieldParamList = new LinkedList<>();

    @ApiModelProperty(value = "表字段删除信息")
    private List<String> delTableFieldParamList = new LinkedList<>();

    @ApiModelProperty(value = "新增外建")
    private List<AlterTableParamCondition.ForeignParam>  addForeignParamList;

    @ApiModelProperty(value = "删除外建")
    private List<AlterTableParamCondition.ForeignParam>  delForeignParamList;

    @ApiModelProperty(value = "新增分区")
    private PartitionInfo addPartition;

    @ApiModelProperty(value = "新增多级分区")
    private MultiLevelPartitionInfo addMultiLevelPartition;

    @ApiModelProperty(value = "删除分区")
    private PartitionInfo delPartition;

    @ApiModelProperty(value = "删除多级分区")
    private MultiLevelPartitionInfo delMultiLevelPartition;

    @ApiModelProperty(value = "新表名称")
    private String newTableName;

    @ApiModelProperty(value = "是否删除主键")
    private boolean delPrimaryKey;

    @ApiModelProperty(value = "表描述")
    private String tableComment;

    @ApiModelProperty(value = "表类型(内部表/外部表)", notes = "true:外部表;false:内部表, 默认为true")
    private boolean external = true;
}
