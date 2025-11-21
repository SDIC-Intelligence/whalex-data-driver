package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @author 黄河森
 * @date 2022/7/18
 * @package com.meiya.whalex.db.entity.ani
 * @project whalex-data-driver8
 */
public enum EngineType {
    /**
     * MergeTree 系列引擎：被设计用于插入极大量的数据到一张表中，数据可以以数据片段的形式一个接着一个的快速写入
     * 数据片段在后台按照一定规则进行合并。相比在插入时不断修改（重写）已存储的数据，这种策略高效很多。
     */
    MERGE_TREE("MergeTree", "MergeTree()"),
    VERSIONED_COLLAPSING_MERGE_TREE("VersionedCollapsingMergeTree", "VersionedCollapsingMergeTree(%s, %s)"),
    GRAPHITE_MERGE_TREE("GraphiteMergeTree", "GraphiteMergeTree(%s)"),
    AGGREGATING_MERGE_TREE("AggregatingMergeTree", "AggregatingMergeTree()"),
    COLLAPSING_MERGE_TREE("CollapsingMergeTree", "AggregatingMergeTree(%s)"),
    /**
     * 具备排重功能的引擎
     */
    REPLACING_MERGE_TREE("ReplacingMergeTree", "ReplacingMergeTree(%s)"),
    /**
     * 合并相同主键的行
     */
    SUMMING_MERGE_TREE("SummingMergeTree", "SummingMergeTree(%s)"),
    /**
     * 日志引擎
     */
    LOG("Log", "Log"),
    STRIPE_LOG("StripeLog", "StripeLog"),
    /**
     * 最简单的表引擎，存储小数据量
     */
    TINY_LOG("TinyLog", "TinyLog")
    ;

    private String engineName;

    private String engineSql;

    public String getEngineName() {
        return engineName;
    }

    public String getEngineSql() {
        return engineSql;
    }

    EngineType(String engineName, String engineSql) {
        this.engineName = engineName;
        this.engineSql = engineSql;
    }

    public static EngineType findEngineType(String engine) {
        if (StringUtils.isBlank(engine)) {
            return EngineType.TINY_LOG;
        }
        EngineType result = null;
        for (EngineType engineType : EngineType.values()) {
            if (engine.equalsIgnoreCase(engineType.engineName)) {
                result =  engineType;
                break;
            }
        }
        if (result == null) {
            throw new BusinessException("ClickHouse 无法识别当前引擎类型: " + engine);
        }
        return result;
    }

    public static String parserEngineSQL(EngineType engineType, Map<String, String> engineParamMap) {
        switch (engineType) {
            case TINY_LOG:
            case LOG:
            case STRIPE_LOG:
            case AGGREGATING_MERGE_TREE:
            case MERGE_TREE:
                return engineType.getEngineName();
            case VERSIONED_COLLAPSING_MERGE_TREE:
                String sign = engineParamMap.get("sign");
                String version = engineParamMap.get("version");
                if (StringUtils.isAnyBlank(sign, version)) {
                    throw new BusinessException("ClickHouse 创建引擎为：" + VERSIONED_COLLAPSING_MERGE_TREE.getEngineName() + " 的表需要配置[sign,version]参数");
                }
                return String.format(VERSIONED_COLLAPSING_MERGE_TREE.getEngineSql(), sign, version);
            case GRAPHITE_MERGE_TREE:
                String config = engineParamMap.get("config_section");
                if (StringUtils.isBlank(config)) {
                    throw new BusinessException("ClickHouse 创建引擎为：" + GRAPHITE_MERGE_TREE.getEngineName() + " 的表需要配置[config_section]参数");
                }
                return String.format(GRAPHITE_MERGE_TREE.getEngineSql(), config);
            case COLLAPSING_MERGE_TREE:
                sign = engineParamMap.get("sign");
                if (StringUtils.isBlank(sign)) {
                    throw new BusinessException("ClickHouse 创建引擎为：" + COLLAPSING_MERGE_TREE.getEngineName() + " 的表需要配置[sign]参数");
                }
                return String.format(COLLAPSING_MERGE_TREE.getEngineSql(), sign);
            case REPLACING_MERGE_TREE:
                version = engineParamMap.get("version");
                if (StringUtils.isNotBlank(version)) {
                    return String.format(REPLACING_MERGE_TREE.getEngineSql(), version);
                } else {
                    return String.format(REPLACING_MERGE_TREE.getEngineSql(), "");
                }
            case SUMMING_MERGE_TREE:
                String columns = engineParamMap.get("columns");
                if (StringUtils.isNotBlank(columns)) {
                    return String.format(SUMMING_MERGE_TREE.getEngineSql(), columns);
                } else {
                    return String.format(SUMMING_MERGE_TREE.getEngineSql(), "");
                }
            default:
                throw new BusinessException("无法解析当前引擎：" + engineType.getEngineName());
        }
    }
}
