package com.meiya.whalex.sql2dsl.entity;

import com.meiya.whalex.interior.db.builder.AggregateBuilder;
import com.meiya.whalex.interior.db.search.condition.AggOpType;
import lombok.Builder;
import lombok.Data;

/**
 * date_histogram
 *
 * @author 黄河森
 * @date 2024/1/2
 * @package com.meiya.whalex.sql2dsl.entity
 * @project whalex-data-driver
 * @description SqlGroupAgg
 */
@Data
public class SqlDateHistogramAgg extends SqlAgg {

    private AggregateBuilder.HistogramDateFormat format;

    private Integer intervalNum;

    private AggregateBuilder.HistogramDateType histogramDateType;

    @Builder(toBuilder = true)
    public SqlDateHistogramAgg(String aggName, String fieldName, AggregateBuilder.HistogramDateFormat format, Integer intervalNum, AggregateBuilder.HistogramDateType histogramDateType) {
        super(aggName, fieldName, AggOpType.DATE_HISTOGRAM);
        this.format = format;
        this.intervalNum = intervalNum;
        this.histogramDateType = histogramDateType;
    }
}
