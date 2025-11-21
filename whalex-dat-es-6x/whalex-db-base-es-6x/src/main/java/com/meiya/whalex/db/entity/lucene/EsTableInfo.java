package com.meiya.whalex.db.entity.lucene;

import com.meiya.whalex.db.entity.AbstractDbTableInfo;

import java.util.Map;

/**
 * 数据库表配置信息
 *
 * @author 黄河森
 * @date 2019/12/19
 * @project whale-cloud-platformX
 */
public class EsTableInfo extends AbstractDbTableInfo {

    /**
     * 文档类型
     */
    private String docType = "_doc";

    /**
     * solr的周期类型, y-年, m-月, d-天
     */
    private String periodType;

    /**
     * 存储的周期值
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
     * schema 类型
     */
    private String schema;

    /**
     * 分片的数量
     */
    private int burstZoneNum;

    /**
     * es滚动策略配置
     */
    private EsRolloverPolicy esRolloverPolicy;

    /**
     * 是否忽略不存在的索引
     */
    private Boolean ignoreUnavailable = true;

    /**
     * 是否切分查询，比如周期表一共 3600 个索引，是否分批次查询
     *
     * 默认 false，即直接 * 匹配全部
     */
    private Boolean segmentationQuery = false;

    /**
     * 是否创建模版
     */
    private Boolean createTemplate;

    /**
     * 切分数量
     */
    private Integer segmentalQuantity = 10;

    private Integer totalShardsPerNode;

    private Integer numberOfShards;

    private Integer replicas = 1;

    public Integer getReplicas() {
        return replicas;
    }

    public void setReplicas(Integer replicas) {
        this.replicas = replicas;
    }

    public Integer getRefreshInterval() {
        return refreshInterval;
    }

    public void setRefreshInterval(Integer refreshInterval) {
        this.refreshInterval = refreshInterval;
    }

    /**
     * 定时刷新时间
     */
    private Integer refreshInterval;

    public Integer getTotalShardsPerNode() {
        return totalShardsPerNode;
    }

    public void setTotalShardsPerNode(Integer totalShardsPerNode) {
        this.totalShardsPerNode = totalShardsPerNode;
    }

    public Integer getNumberOfShards() {
        return numberOfShards;
    }

    public void setNumberOfShards(Integer numberOfShards) {
        this.numberOfShards = numberOfShards;
    }

    public EsRolloverPolicy getEsRolloverPolicy() {
        return esRolloverPolicy;
    }

    public void setEsRolloverPolicy(EsRolloverPolicy esRolloverPolicy) {
        this.esRolloverPolicy = esRolloverPolicy;
    }

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

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public int getBurstZoneNum() {
        return burstZoneNum;
    }

    public void setBurstZoneNum(int burstZoneNum) {
        this.burstZoneNum = burstZoneNum;
    }

    public Boolean getIgnoreUnavailable() {
        return ignoreUnavailable;
    }

    public void setIgnoreUnavailable(Boolean ignoreUnavailable) {
        this.ignoreUnavailable = ignoreUnavailable;
    }

    public String getDocType() {
        return docType;
    }

    public void setDocType(String docType) {
        this.docType = docType;
    }

    public Boolean getSegmentationQuery() {
        return segmentationQuery;
    }

    public void setSegmentationQuery(Boolean segmentationQuery) {
        this.segmentationQuery = segmentationQuery;
    }

    public Boolean isCreateTemplate() {
        return createTemplate;
    }

    public void setCreateTemplate(boolean createTemplate) {
        this.createTemplate = createTemplate;
    }


    public Integer getSegmentalQuantity() {
        return segmentalQuantity;
    }

    public void setSegmentalQuantity(Integer segmentalQuantity) {
        this.segmentalQuantity = segmentalQuantity;
    }

    /**
     * es 滚动索引策略
     */
    public static class EsRolloverPolicy {

        /**
         * 一张索引表存储的大小（1tb）
         */
        private String maxSize;

        /**
         * 索引表文档的数量
         */
        private Integer maxDocs;

        /**
         * 索引表滚动的日期（1d）
         */
        private String maxAge;

        public EsRolloverPolicy(String maxSize, Integer maxDocs, String maxAge) {
            this.maxSize = maxSize;
            this.maxDocs = maxDocs;
            this.maxAge = maxAge;
        }

        public EsRolloverPolicy() {
        }

        public String getMaxSize() {
            return maxSize;
        }

        public void setMaxSize(String maxSize) {
            this.maxSize = maxSize;
        }

        public Integer getMaxDocs() {
            return maxDocs;
        }

        public void setMaxDocs(Integer maxDocs) {
            this.maxDocs = maxDocs;
        }

        public String getMaxAge() {
            return maxAge;
        }

        public void setMaxAge(String maxAge) {
            this.maxAge = maxAge;
        }
    }
}
