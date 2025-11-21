package com.meiya.whalex.interior.db.search.condition;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;

/**
 * 操作参数枚举
 *
 * @author 黄河森
 * @date 2019/9/10
 * @project whale-cloud-platformX
 */
public enum Rel implements Serializable {
    AND("and"),
    OR("or"),
    NOT("not"),
    EQ("eq"),
    NE("ne"),
    GT("gt"),
    GTE("gte"),
    LT("lt"),
    LTE("lte"),
    IN("in"),
    NIN("nin"),
    NULL("null"),
    NOT_NULL("nnull"),
    NOT_FRONT_LIKE("not%like"),
    NOT_MIDDLE_LIKE("notmiddleLike"),
    NOT_TAIL_LIKE("notlike%"),
    NESTED("nested"),
    BETWEEN("between"),
    /**
     * 入参不需要包含 * 或者 % 号，自动加上 %val%
     */
    LIKE("like"),
    /**
     * 入参不需要包含 * 或者 % 号，自动加上 %val
     */
    FRONT_LIKE("%like"),
    /**
     * 入参需要包含 * 或者 % 号，也允许 ?
     */
    MIDDLE_LIKE("middleLike"),
    ILIKE("ilike"),
    /**
     * 入参不需要包含 * 或者 % 号，自动加上 val%
     */
    TAIL_LIKE("like%"),
    ONLY_LIKE("like?"),
    MATCH("match"),
    MATCH_ALL("match_all"),
    MATCH_PHRASE("match_phrase"),
    MULTI_MATCH("multi_match"),
    EXISTS("exists"),
    REGEX("regex"),
    OPTIONS("options"),
    TERM("term"),

    /**
     * Solr 特有的查询类型
     */
    /**
     * 单字段统计
     */
    @Deprecated
    FACET_FIELD("facet.field"),

    /**
     * 单字段统计
     */
    @Deprecated
    GROUP_FIELD("group.field"),

    /**
     * Solr 特有的查询类型
     */
    @Deprecated
    STATS_FIELD("stats.field"),

    /**
     * Solr 特有的查询类型
     */
    @Deprecated
    STATS_FACET("stats.facet"),

    /**
     * 用户自定义查询
     */
    @Deprecated
    FACET_QUERY("facet.query"),

    /**
     * 多字段统计
     */
    @Deprecated
    FACET_PIVOT("facet.pivot"),

    /**
     * solr 过滤条件
     */
    @Deprecated
    FQ("fq"),

    /**
     * 地理位置查询, 圆形
     * 对应的paramValue是Map对象，至少有两个参数，
     * distance;//eg:distance:10km 半径
     * loc;loc:"23.04805756,113.27598616"//圆点的中心值
     */
    GEO_DISTANCE("geo_distance"),
    /**
     * 地理位置查询,长方形
     * 对应的paramValue是Map对象，至少有两个参数，
     * topLeft;//左上角坐标
     * bottomRight;//右下角坐标
     */
    GEO_BOX("geo_bounding_box"),
    /**
     * 对字段值做长度限制
     */
    LENGTH("length"),

    /**
     * 地理位置查询多边形
     * [{"lat":"0.151","lon":"254.00"}, {"lat":"1.555","lon":"85.55"}, {"lat":"1.555","lon":"85.55"}]
     */
    GEO_POLYGON("geo_polygon"),

    KEYS("keys"),
    HKEYS("hkeys"),
    HGET("hget"),
    HDEL("hdel"),
    HSET("hset"),
    MGET("mget"),
    HMGET("hmget"),
    TTL("ttl"),

    PARENT_ID("parent_id"),
    HAS_PARENT("has_parent"),
    HAS_CHILD("has_child"),
    ;

    private final String name;

    Rel(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    @JsonCreator
    public static Rel parse(String name) {
        if (name == null) {
            return Rel.AND;
        }
        for (Rel rel : Rel.values()) {
            if (rel.name.equalsIgnoreCase(name) || rel.name().equalsIgnoreCase(name)) {
                return rel;
            }
        }
        throw new RuntimeException("未知的操作符：" + name);
    }

    @Override
    public String toString() {
        return getName();
    }
}
