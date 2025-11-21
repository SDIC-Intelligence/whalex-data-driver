package com.meiya.whalex.interior.db.search.in;

import com.meiya.whalex.interior.db.search.condition.AggFunctionType;
import com.meiya.whalex.interior.db.search.condition.Rel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;

/**
 * 聚合函数语法
 *
 * @author 黄河森
 * @date 2020/12/15
 * @project whalex-data-driver
 */
@Data
public class AggFunction implements Serializable {

    @ApiModelProperty(value = "聚合函数结果别名")
    private String functionName;

    @ApiModelProperty(value = "聚合函数")
    private AggFunctionType aggFunctionType;

    @ApiModelProperty(value = "聚合字段")
    private String field;

    @ApiModelProperty(value = "聚合权重字段")
    private String weightField;

    @ApiModelProperty(value = "聚合百分比数组")
    private List<Integer> percentiles;

    @ApiModelProperty(value = "当前聚合函数命中的前 N 条数据", notes = "若设置此值，则会返回当前参与聚合函数的前N条数据")
    private Integer hitNum;

    public AggFunction() {
    }

    public AggFunction(String functionName, AggFunctionType aggFunctionType, String field) {
        this.functionName = functionName;
        this.aggFunctionType = aggFunctionType;
        this.field = field;
    }

    public AggFunction(String functionName, AggFunctionType aggFunctionType, String field, String weightField) {
        this.functionName = functionName;
        this.aggFunctionType = aggFunctionType;
        this.field = field;
        this.weightField = weightField;
    }

    public AggFunction(String functionName, AggFunctionType aggFunctionType, String field, List<Integer> percentiles) {
        this.functionName = functionName;
        this.aggFunctionType = aggFunctionType;
        this.field = field;
        this.percentiles = percentiles;
    }

    public static AggFunction create(String functionName, String field, AggFunctionType type) {
        return new AggFunction(functionName, type, field);
    }

    public static AggFunction createPercentiles(String functionName, String field, List<Integer> percentiles) {
        return new AggFunction(functionName, AggFunctionType.PERCENTILES, field, percentiles);
    }

    public static AggFunction createAvgWeight(String functionName, String field, String weightField) {
        return new AggFunction(functionName, AggFunctionType.AVG, field, weightField);
    }

    /**
     * 若设置 hitNum 值，则返回报文中对应的key名称
     *
     * @return
     */
    public String getHitName() {
        String hitName = getFunctionName() + "_hit";
        return hitName;
    }

    public String getFunctionName() {
        if (StringUtils.isBlank(functionName) && StringUtils.isNotBlank(field)) {
            if (aggFunctionType != null) {
                functionName = field + "_" + aggFunctionType.getType();
            } else {
                functionName = field + "_agg";
            }
        }
        return functionName;
    }
}
