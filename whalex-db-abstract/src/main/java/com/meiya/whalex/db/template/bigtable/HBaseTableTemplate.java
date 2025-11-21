package com.meiya.whalex.db.template.bigtable;

import com.meiya.whalex.interior.db.constant.PeriodCycleEnum;
import lombok.Builder;
import lombok.Data;

/**
 * @author 黄河森
 * @date 2020/7/6
 * @project whalex-data-driver
 */
@Data
@Builder
public class HBaseTableTemplate {

    private String schema;

    private PeriodCycleEnum periodType;

    private String storePeriodValue;

    private String prePeriod;

    private String backwardPeriod;

    private String numregions;

    public HBaseTableTemplate() {
    }

    public HBaseTableTemplate(String schema, String periodType, String storePeriodValue, String prePeriod, String backwardPeriod, String numregions) {
        this.schema = schema;
        this.periodType = PeriodCycleEnum.parse(periodType);
        this.storePeriodValue = storePeriodValue;
        this.prePeriod = prePeriod;
        this.backwardPeriod = backwardPeriod;
        this.numregions = numregions;
    }

    public HBaseTableTemplate(String schema, PeriodCycleEnum periodType, String storePeriodValue, String prePeriod, String backwardPeriod, String numregions) {
        this.schema = schema;
        this.periodType = periodType;
        this.storePeriodValue = storePeriodValue;
        this.prePeriod = prePeriod;
        this.backwardPeriod = backwardPeriod;
        this.numregions = numregions;
    }
}
