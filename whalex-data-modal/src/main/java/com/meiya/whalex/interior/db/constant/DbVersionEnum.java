package com.meiya.whalex.interior.db.constant;

import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

/**
 * 组件版本注解
 *
 * @author 蔡荣桂
 * @date 2021/7/26
 * @project whale-cloud-platformX
 */
public enum DbVersionEnum {

    ES_6(DbResourceEnum.es, "6"),
    ES_7(DbResourceEnum.es, "7"),
    HADOOP_2_7_2(DbResourceEnum.hadoop, "2.7.2"),
    HADOOP_3_1_0(DbResourceEnum.hadoop, "3.1.0"),
    HADOOP_3_1_1(DbResourceEnum.hadoop, "3.1.1"),
    HADOOP_3_1_1_MRS(DbResourceEnum.hadoop, "3.1.1.MRS"),
    HBASE_1_2_1(DbResourceEnum.hbase, "1.2.1"),
    HBASE_1_3_1(DbResourceEnum.hbase, "1.3.1"),
    HBASE_2_0_0(DbResourceEnum.hbase, "2.0.0"),
    HIVE_1_2_1(DbResourceEnum.hive, "1.2.1"),
    HIVE_2_1_1(DbResourceEnum.hive, "2.1.1"),
    HIVE_2_2_0(DbResourceEnum.hive, "2.2.0"),
    HIVE_3_1_0(DbResourceEnum.hive, "3.1.0"),
    HIVE_MRS(DbResourceEnum.hive, "mrs"),
    KAFKA_1_1_0(DbResourceEnum.kafka, "1.1.0"),
    KAFKA_2_4_0(DbResourceEnum.kafka, "2.4.0"),
    POSTGRE_9_3(DbResourceEnum.postgre, "9.3"),
    VERTICA_10_0_1(DbResourceEnum.vertica, "10.0.1-0"),
    MYSQL_8_0_17(DbResourceEnum.mysql, "8.0.17"),
    ORACLE_11G(DbResourceEnum.oracle, "11g"),
    ORACLE_12C(DbResourceEnum.oracle, "12c"),
    S3_1_11_415(DbResourceEnum.s3, "1.11.415"),
    MONGO_3_0_1(DbResourceEnum.mongodb, "3.0.1"),
    MONGO_3_8_2(DbResourceEnum.mongodb, "3.8.2"),
    MONGO_4_0_5(DbResourceEnum.mongodb, "4.0.5"),
    NEO4J_4_4_11(DbResourceEnum.neo4j, "4.4.11"),
    REDIS_6(DbResourceEnum.redis, "6"),
    DORIS_0_14(DbResourceEnum.doris, "0.14"),
    DM_7(DbResourceEnum.dm, "7"),
    GBASE_8_6_2_43(DbResourceEnum.gbase, "8.6.2.43"),
    CLICKHOUSE_22_2_2_1(DbResourceEnum.clickhouse, "22.2.2.1"),
    KINGBASEES_8_6_0(DbResourceEnum.kingbasees, "8.6.0"),
    JANUSGRAPH_0_2_0(DbResourceEnum.janusgraph, "0.2.0"),
    NEBULAGRAPH_3_8_0(DbResourceEnum.nebulagraph, "3.8.0"),
    HIGHGO_1_2(DbResourceEnum.highgo, "6.0.4"),
    GREMLIN_3_7_3(DbResourceEnum.gremlin, "3.7.3"),

    UNDEFINE(null, "UNDEFINE");

    private DbResourceEnum dbType;

    private String version;

    DbVersionEnum(DbResourceEnum dbType, String version) {
        this.dbType = dbType;
        this.version = version;
    }

    public DbResourceEnum getDbType() {
        return dbType;
    }

    @JsonValue
    public String getVersion() {
        return version;
    }

    public static DbVersionEnum findDbVersionEnum(String value) {
        if (StringUtils.isBlank(value)) {
            return DbVersionEnum.UNDEFINE;
        }
        DbVersionEnum result = null;
        for (DbVersionEnum dbVersionEnum : DbVersionEnum.values()) {
            if (value.equalsIgnoreCase(dbVersionEnum.version)) {
                result =  dbVersionEnum;
                break;
            }
        }
        if (result == null) {
            result = UNDEFINE;
        }
        return result;
    }

}
