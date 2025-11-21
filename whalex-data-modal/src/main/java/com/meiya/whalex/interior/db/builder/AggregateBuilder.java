package com.meiya.whalex.interior.db.builder;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.interior.db.search.condition.AggOpType;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.interior.db.search.in.AggFunction;
import com.meiya.whalex.interior.db.search.in.AggHit;
import com.meiya.whalex.interior.db.search.in.Aggs;
import com.meiya.whalex.interior.db.search.in.Order;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 黄河森
 * @date 2021/6/18
 * @project whalex-data-driver-back
 */
public class AggregateBuilder {

    private Aggs aggs;

    private AggregateBuilder() {
        this.aggs = new Aggs();
    }

    public static AggregateBuilder builder() {
        return new AggregateBuilder();
    }

    /**
     * 分组聚合
     *
     * @param aggName
     * @param field
     * @return
     */
    public NestFunctionBuilder group(String aggName, String field) {
        this.aggs.setType(AggOpType.GROUP);
        this.aggs.setField(field);
        this.aggs.setAggName(aggName);
        return new NestFunctionBuilder(aggs);
    }


    public GroupAggBuilder nested(String aggName) {
        this.aggs.setType(AggOpType.NESTED);
        this.aggs.setAggName(aggName);
        return new GroupAggBuilder(this.aggs);
    }

    public GroupAggBuilder group(String aggName) {
        this.aggs.setType(AggOpType.GROUP);
        this.aggs.setAggName(aggName);
        return new GroupAggBuilder(this.aggs);
    }

    public DistinctAggBuilder distinct(String aggName) {
        this.aggs.setType(AggOpType.DISTINCT);
        this.aggs.setAggName(aggName);
        return new DistinctAggBuilder(this.aggs);
    }

    /**
     * 直方图聚合
     *
     * @param aggName
     * @param field
     * @param interval
     * @return
     */
    public NestFunctionBuilder histogram(String aggName, String field, String interval) {
        this.aggs.setType(AggOpType.HISTOGRAM);
        this.aggs.setField(field);
        this.aggs.setAggName(aggName);
        this.aggs.setInterval(interval);
        return new NestFunctionBuilder(aggs);
    }
    /**
     * 日期直方图聚合
     *
     * @param aggName
     * @param field
     * @param intervalNum
     * @param histogramDateType
     * @param histogramDateFormat
     * @return
     */
    public NestFunctionBuilder histogramForDate(String aggName, String field, Integer intervalNum, HistogramDateType histogramDateType, HistogramDateFormat histogramDateFormat) {
        return this.histogramForDate(aggName, field, intervalNum, histogramDateType, histogramDateFormat, null);
    }

    /**
     * 日期直方图聚合
     *
     * @param aggName
     * @param field
     * @param intervalNum
     * @param histogramDateType
     * @param histogramDateFormat
     * @return
     */
    public NestFunctionBuilder histogramForDate(String aggName, String field, Integer intervalNum, HistogramDateType histogramDateType, HistogramDateFormat histogramDateFormat, String timeZone) {
        this.aggs.setType(AggOpType.DATE_HISTOGRAM);
        this.aggs.setField(field);
        this.aggs.setAggName(aggName);
        if (histogramDateType == null) {
            throw new IllegalArgumentException("histogramDateType 时间间隔单位参数不能为空!");
        }
        String interval;
        if (intervalNum != null) {
            interval = intervalNum + histogramDateType.abbreviation;
        } else {
            interval = histogramDateType.type;
        }
        this.aggs.setInterval(interval);
        this.aggs.setFormat(histogramDateFormat.getFormat());
        if (StringUtils.isNotBlank(timeZone)) {
            this.aggs.setTimeZone(timeZone);
        }
        return new NestFunctionBuilder(aggs);
    }

    /**
     * 范围聚合
     *
     * @param aggName
     * @return
     */
    public RangeAggBuilder range(String aggName) {
        this.aggs.setType(AggOpType.RANGE);
        this.aggs.setAggName(aggName);
        return new RangeAggBuilder(aggs);
    }

    /**
     * 聚合函数
     */
    public static class NestFunctionBuilder {
        private Aggs agg;

        private NestFunctionBuilder(Aggs agg) {
            this.agg = agg;
        }

        public NestFunctionBuilder function(AggFunction aggFunction) {
            this.agg.addFunction(aggFunction);
            return this;
        }

        public NestAggHitBuilder hits() {
            AggHit aggHit = new AggHit();
            this.agg.setAggHit(aggHit);
            return new NestAggHitBuilder(this, aggHit);
        }

        public Aggs build() {
            return this.agg;
        }
    }

    /**
     * 日期直方图日期格式化类型
     */
    public enum HistogramDateFormat {
        NORM_DATE_PATTERN("yyyy-MM-dd"),
        NORM_DATE_YEAR_PATTERN("yyyy"),
        NORM_DATE_MONTH_PATTERN("yyyy-MM"),
        NORM_DATETIME_MINUTE_PATTERN("yyyy-MM-dd HH:mm"),
        NORM_DATETIME_HOUR_PATTERN("yyyy-MM-dd HH"),
        NORM_DATETIME_PATTERN("yyyy-MM-dd HH:mm:ss"),
        ;

        /**
         * 格式
         */
        private String format;

        HistogramDateFormat(String format) {
            this.format = format;
        }

        public String getFormat() {
            return format;
        }
    }

    /**
     * 日期直方图间隔类型
     */
    public enum HistogramDateType {
        DAY("day", "d"),
        HOUR("hour", "h"),
        MINUTE("minute", "m"),
        QUARTER("quarter", "q"),
        SECOND("SECOND", "s"),
        WEEK("week", "w"),
        YEAR("year", "y"),
        MONTH("month", "M")
        ;

        /**
         * 格式
         */
        private String type;

        /**
         * 缩写
         */
        private String abbreviation;

        HistogramDateType(String type, String abbreviation) {
            this.type = type;
            this.abbreviation = abbreviation;
        }

        public String getType() {
            return type;
        }

        public String getAbbreviation() {
            return abbreviation;
        }

        public static HistogramDateType getHistogramDateTypeByValue(String value) {
            for (HistogramDateType histogramDateType : HistogramDateType.values()) {
                if(histogramDateType.type.equalsIgnoreCase(value) || histogramDateType.abbreviation.equalsIgnoreCase(value)) {
                    return histogramDateType;
                }
            }
            throw new RuntimeException("未知的日期直方图间隔类型");
        }
    }

    /**
     * 分组聚合
     */
    public static class GroupAggBuilder {
        private Aggs agg;

        private GroupAggBuilder(Aggs agg) {
            this.agg = agg;
        }

        public GroupAggBuilder field(String field) {
            this.agg.setField(field);
            return this;
        }

        public GroupAggBuilder order(String field, Sort sort) {
            List<Order> orders = this.agg.getOrders();
            if (orders == null) {
                orders = new ArrayList<>();
                this.agg.setOrders(orders);
            }
            orders.add(Order.create(field, sort));
            return this;
        }

        public GroupAggBuilder limit(Integer limitNum) {
            this.agg.setLimit(limitNum);
            return this;
        }

        public GroupAggBuilder minCount(Integer minCount) {
            this.agg.setMincount(minCount);
            return this;
        }


        @Deprecated
        public GroupAggBuilder hits(Integer hitNum) {
            this.agg.setHitNum(hitNum);
            return this;
        }

        public AggHitBuilder hits() {
            AggHit aggHit = new AggHit();
            this.agg.setAggHit(aggHit);
            return new AggHitBuilder(this, aggHit);
        }

        public GroupAggBuilder function(AggFunction aggFunction) {
            this.agg.addFunction(aggFunction);
            return this;
        }

        public GroupAggBuilder subAgg(Aggs agg) {
            List<Aggs> aggList = this.agg.getAggList();
            if (aggList == null) {
                aggList = new ArrayList<>();
                this.agg.setAggList(aggList);
            }
            aggList.add(agg);
            return this;
        }

        public Aggs build() {
            return this.agg;
        }
    }

    /**
     * 排重聚合
     */
    public static class DistinctAggBuilder {
        private Aggs agg;

        private DistinctAggBuilder(Aggs agg) {
            this.agg = agg;
        }

        public DistinctAggBuilder field(String field) {
            this.agg.setField(field);
            return this;
        }

        public DistinctAggBuilder hits(Integer hitNum) {
            this.agg.setHitNum(hitNum);
            return this;
        }

        public Aggs build() {
            return this.agg;
        }
    }

    /**
     * 范围查询构建
     */
    public static class RangeAggBuilder {
        private Aggs agg;

        private RangeAggBuilder(Aggs agg) {
            this.agg = agg;
        }

        public RangeAggBuilder field(String field) {
            this.agg.setField(field);
            return this;
        }

        public RangeKeyBuilder rangeParam(String key) {
            List<Aggs.RangeKey> rangeKeys = this.agg.getRangeKeys();
            if (rangeKeys == null) {
                rangeKeys = new ArrayList<>();
                this.agg.setRangeKeys(rangeKeys);
            }
            Aggs.RangeKey rangeKey = new Aggs.RangeKey();
            rangeKey.setKey(key);
            rangeKeys.add(rangeKey);
            return new RangeKeyBuilder(this, rangeKey);
        }

        public RangeAggBuilder function(AggFunction aggFunction) {
            this.agg.addFunction(aggFunction);
            return this;
        }

        public RangeAggBuilder subAgg(Aggs agg) {
            List<Aggs> aggList = this.agg.getAggList();
            if (aggList == null) {
                aggList = new ArrayList<>();
                this.agg.setAggList(aggList);
            }
            aggList.add(agg);
            return this;
        }

        public Aggs build() {
            return this.agg;
        }
    }

    /**
     * 范围查询条件
     */
    public static class RangeKeyBuilder {
        private Aggs.RangeKey rangeKey;
        private RangeAggBuilder builder;

        private RangeKeyBuilder(RangeAggBuilder builder, Aggs.RangeKey rangeKey) {
            this.builder = builder;
            this.rangeKey = rangeKey;
        }

        public RangeKeyBuilder from(Object from) {
            this.rangeKey.setFrom(from);
            return this;
        }

        public RangeKeyBuilder to(Object to) {
            this.rangeKey.setTo(to);
            return this;
        }

        public RangeAggBuilder returned() {
            return this.builder;
        }
    }

    /**
     * 参与聚合的数据检索
     */
    public static class BaseAggHitBuilder<U extends BaseAggHitBuilder, T> {
        private T builder;
        private AggHit aggHit;

        private BaseAggHitBuilder(T builder, AggHit aggHit) {
            this.builder = builder;
            this.aggHit = aggHit;
        }

        public U size(Integer size) {
            this.aggHit.setSize(size);
            return (U) this;
        }

        public U select(String... select) {
            List<String> fields = this.aggHit.getSelect();
            if (fields == null) {
                this.aggHit.setSelect(CollectionUtil.newArrayList(select));
            } else {
                fields.addAll(CollectionUtil.newArrayList(select));
            }
            return (U) this;
        }

        public U order(String field, Sort sort) {
            List<Order> orders = this.aggHit.getOrders();
            if (orders == null) {
                orders = new ArrayList<>();
                this.aggHit.setOrders(orders);
            }
            orders.add(Order.create(field, sort));
            return (U) this;
        }

        public T returned() {
            return this.builder;
        }
    }

    public static class AggHitBuilder extends BaseAggHitBuilder<AggHitBuilder, GroupAggBuilder>{

        private AggHitBuilder(GroupAggBuilder builder, AggHit aggHit) {
            super(builder, aggHit);
        }
    }

    public static class NestAggHitBuilder extends BaseAggHitBuilder<NestAggHitBuilder, NestFunctionBuilder>{

        private NestAggHitBuilder(NestFunctionBuilder builder, AggHit aggHit) {
            super(builder, aggHit);
        }
    }
}
