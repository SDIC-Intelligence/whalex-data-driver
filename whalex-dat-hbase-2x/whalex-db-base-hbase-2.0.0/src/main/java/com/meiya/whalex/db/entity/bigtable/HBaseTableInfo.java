package com.meiya.whalex.db.entity.bigtable;

import com.meiya.whalex.db.entity.AbstractDbTableInfo;

/**
 * HBase 表配置信息
 *
 * @author 黄河森
 * @date 2019/12/16
 * @project whale-cloud-platformX
 */
public class HBaseTableInfo extends AbstractDbTableInfo {

    /**
     * HBase的周期类型, y-年, m-月, d-天
     */
    private String periodType;

    /**
     * HBase的周期值
     */
    private Integer periodValue;

    /**
     * 往后的周期值
     */
    private Integer futurePeriodValue = 3;

    /**
     * 过往周期值
     */
    private Integer prePeriodValue = 7;

    /**
     * 分表数量
     */
    private Integer regionCount = 10;

    /**
     * 表结构
     */
    private String schema;

    public String getPeriodType() {
        return periodType;
    }

    public void setPeriodType(String periodType) {
        this.periodType = periodType;
    }

    public Integer getPeriodValue() {
        return periodValue;
    }

    public void setPeriodValue(Integer periodValue) {
        this.periodValue = periodValue;
    }

    public Integer getFuturePeriodValue() {
        return futurePeriodValue;
    }

    public void setFuturePeriodValue(Integer futurePeriodValue) {
        this.futurePeriodValue = futurePeriodValue;
    }

    public Integer getPrePeriodValue() {
        if (prePeriodValue == null) {
            return periodValue;
        }
        return prePeriodValue;
    }

    public void setPrePeriodValue(Integer prePeriodValue) {
        this.prePeriodValue = prePeriodValue;
    }

    public Integer getRegionCount() {
        return regionCount;
    }

    public void setRegionCount(Integer regionCount) {
        this.regionCount = regionCount;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
