package com.meiya.whalex.interior.db.constant;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 存储服务组件枚举
 *
 * @author Huanghesen
 * @date  2018-9-5 10:31:29
 */
public enum DbResourceEnum {

    /**
     * MONGODB
     */
    mongodb("01", true),
    /**
     * PostGrep
     */
    postgre("07", true),
    /**
     * Hadoop
     */
    hadoop("08", false),
    /**
     * HBase
     */
    hbase("09", false),
    /**
     * Mysql
     */
    mysql("10", true),
    /**
     * ES
     */
    es("12", false, "6", "7"),
    /**
     * Redis
     */
    redis("13", false),
    /**
     * ftp
     */
    ftp("16", false),
    /**
     * oracle
     */
    oracle("17", true),
    /**
     * greenplum
     */
    greenplum("18", true),
    /**
     * kafka
     */
    kafka("19", false),
    /**
     * hive
     */
    hive("21", false),
    /**
     * clickhouse
     */
    clickhouse("23", true),
    /**
     * s3 对象存储
     */
    s3("26", false),
    /**
     * neo4j
     */
    neo4j("28",true),
    /**
     * Tidy
     */
    tidb("30", true),

    /**
     * doris
     */
    doris("37", true),

    /**
     * DM
     */
    dm("38", true),

    /**
     * gbase
     */
    gbase("39", true),

    /**
     * 人大金仓
     */
    kingbasees("41", true),

    /**
     * vertica
     */
    vertica("42", true),

    /**
     * 开源 janusgraph 图数据库
     */
    janusgraph("45", false),

    /**
     * nebulagraph 图数据库
     */
    nebulagraph("46",false),

    /**
     * 瀚高数据库
     */
    highgo("48", true),

    gremlin("52", false),

    /**
     * 未定义
     */
    undefine("00", false);

    DbResourceEnum(String val, Boolean hasIndex) {
        this.val = val;
        this.hasIndex = hasIndex;
    }

    DbResourceEnum(String val, Boolean hasIndex, String... versions) {
        this.val = val;
        this.hasIndex = hasIndex;
        this.versions = versions;
    }

    private String val;

    /**
     * 是否具备索引
     */
    private Boolean hasIndex;

    /**
     * 版本
     */
    private String[] versions;

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }

    public String getValue() {
        return val;
    }

    public Boolean getHasIndex() {
        return hasIndex;
    }

    public void setHasIndex(Boolean hasIndex) {
        this.hasIndex = hasIndex;
    }

    public String[] getVersions() {
        return versions;
    }

    public void setVersions(String[] versions) {
        this.versions = versions;
    }

    public static DbResourceEnum findDbResourceEnum(String value) {
        if (StringUtils.isBlank(value)) {
            return DbResourceEnum.undefine;
        }
        DbResourceEnum result = null;
        for (DbResourceEnum dbResourceEnum : DbResourceEnum.values()) {
            if (value.equalsIgnoreCase(dbResourceEnum.val)) {
                result =  dbResourceEnum;
                break;
            }
        }
        if (result == null) {
            result = undefine;
        }
        return result;
    }

    public static DbResourceEnum findDbResourceEnumByName(String name) {
        if (StringUtils.isBlank(name)) {
            return DbResourceEnum.undefine;
        }
        DbResourceEnum result = null;
        for (DbResourceEnum dbResourceEnum : DbResourceEnum.values()) {
            if (name.equalsIgnoreCase(dbResourceEnum.name())) {
                result =  dbResourceEnum;
                break;
            }
        }
        if (result == null) {
            result = undefine;
        }
        return result;
    }

    /**
     * 查询具备索引功能的组件
     *
     * @return
     */
    public static List<String> findHasIndexDb() {
        List<String> hasIndexDB = new ArrayList<>();
        for (DbResourceEnum dbResourceEnum : DbResourceEnum.values()) {
            if (dbResourceEnum.hasIndex) {
                hasIndexDB.add(dbResourceEnum.getVal());
            }
        }
        return hasIndexDB;
    }
}
