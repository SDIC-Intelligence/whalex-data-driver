package com.meiya.whalex.db.entity;

public class PeriodTableJsonBean {
    private String periodType;
    private int storePeriodValue;
    private String schema;
    //默认值
    private int prePeriod=7;
    private int backwardPeriod=3;
    private String numregions;

    public String getPeriodType() {
        return periodType;
    }

    public void setPeriodType(String periodType) {
        this.periodType = periodType;
    }

    public int getStorePeriodValue() {
        return storePeriodValue;
    }

    public void setStorePeriodValue(int storePeriodValue) {
        this.storePeriodValue = storePeriodValue;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public int getPrePeriod() {
        return prePeriod;
    }

    public void setPrePeriod(int prePeriod) {
        this.prePeriod = prePeriod;
    }

    public int getBackwardPeriod() {
        return backwardPeriod;
    }

    public void setBackwardPeriod(int backwardPeriod) {
        this.backwardPeriod = backwardPeriod;
    }

    public String getNumregions() {
        return numregions;
    }

    public void setNumregions(String numregions) {
        this.numregions = numregions;
    }

    @Override
    public String toString() {
        return "PeriodTableJsonBean{" +
                "periodType='" + periodType + '\'' +
                ", storePeriodValue=" + storePeriodValue +
                ", schema='" + schema + '\'' +
                ", prePeriod=" + prePeriod +
                ", backwardPeriod=" + backwardPeriod +
                ", numregions=" + numregions +
                '}';
    }
}
