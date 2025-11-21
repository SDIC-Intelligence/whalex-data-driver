package com.meiya.whalex.db.template.lucene;

import com.meiya.whalex.interior.db.constant.PeriodCycleEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * es 数据库表配置
 *
 * @author 黄河森
 * @date 2020/1/15
 * @project whale-cloud-platformX
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
public class EsTableTemplate {

    private PeriodCycleEnum periodType;

    private String storePeriodValue;

    private String numRegions;

    @Deprecated
    private String schema;

    private String backwardPeriod;

    private String prePeriod;

    /**
     * es 滚动索引策略配置
     */
    /**
     * 最大天数
     * n...d,y,m
     */
    private String maxAge;

    /**
     * 最大文档数
     */
    private Integer maxDocs;

    /**
     * 最大存储大小
     * n...kb gb mb
     */
    private String maxSize;

    /**
     * 是否忽略不存在的索引
     */
    private Boolean ignoreUnavailable = true;

    /**
     * 文档类型
     */
    private String docType;

    private Integer refreshInterval;

    private Integer replicas;

    /**
     * 是否切分查询，比如周期表一共 3600 个索引，是否分批次查询
     *
     * 默认 false，即直接 * 匹配全部
     */
    private Boolean segmentationQuery;

    /**
     * 切分数量
     */
    private Integer segmentalQuantity;

    /**
     * 是否创建模版
     */
    private Boolean createTemplate;

    private Integer totalShardsPerNode;

    /**
     * 分区数
     */
    private Integer numberOfShards;

    private Long maxResultWindow;

    public EsTableTemplate() {
    }

    public EsTableTemplate(PeriodCycleEnum periodType) {
        this.periodType = periodType;
    }

    public EsTableTemplate(String periodType) {
        this.periodType = PeriodCycleEnum.parse(periodType);
    }

    public EsTableTemplate(PeriodCycleEnum periodType, String storePeriodValue, String numRegions, String schema, String backwardPeriod, String prePeriod) {
        this.periodType = periodType;
        this.storePeriodValue = storePeriodValue;
        this.numRegions = numRegions;
        this.schema = schema;
        this.backwardPeriod = backwardPeriod;
        this.prePeriod = prePeriod;
    }

    public EsTableTemplate(String periodType, String storePeriodValue, String numRegions, String schema, String backwardPeriod, String prePeriod) {
        this.periodType = PeriodCycleEnum.parse(periodType);
        this.storePeriodValue = storePeriodValue;
        this.numRegions = numRegions;
        this.schema = schema;
        this.backwardPeriod = backwardPeriod;
        this.prePeriod = prePeriod;
    }

    public EsTableTemplate(PeriodCycleEnum periodType, String storePeriodValue, String numRegions, String schema, String backwardPeriod, String prePeriod, String maxAge, Integer maxDocs, String maxSize) {
        this.periodType = periodType;
        this.storePeriodValue = storePeriodValue;
        this.numRegions = numRegions;
        this.schema = schema;
        this.backwardPeriod = backwardPeriod;
        this.prePeriod = prePeriod;
        this.maxAge = maxAge;
        this.maxDocs = maxDocs;
        this.maxSize = maxSize;
    }

    public EsTableTemplate(PeriodCycleEnum periodType, String storePeriodValue, String numRegions, String schema, String backwardPeriod, String prePeriod, String maxAge, Integer maxDocs, String maxSize, Boolean ignoreUnavailable) {
        this.periodType = periodType;
        this.storePeriodValue = storePeriodValue;
        this.numRegions = numRegions;
        this.schema = schema;
        this.backwardPeriod = backwardPeriod;
        this.prePeriod = prePeriod;
        this.maxAge = maxAge;
        this.maxDocs = maxDocs;
        this.maxSize = maxSize;
        this.ignoreUnavailable = ignoreUnavailable;
    }
}
