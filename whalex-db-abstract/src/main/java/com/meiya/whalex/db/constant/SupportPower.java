package com.meiya.whalex.db.constant;

/**
 * 支持的能力
 */
public enum SupportPower {
    TEST_CONNECTION("测试连接"),
    SEARCH("查询"),
    ASSOCIATED_QUERY("关联查询"),
    DELETE("删除"),
    UPDATE("更新"),
    CREATE("新增"),
    CREATE_TABLE("建表"),
    DROP_TABLE("删表"),
    MODIFY_TABLE("修改表"),
    SHOW_DATABASE_LIST("数据库集合"),
    SHOW_TABLE_LIST("当前数据库表的集合"),
    SHOW_SCHEMA("表字段"),
    QUERY_INDEX("查询索引"),
    CREATE_INDEX("创建索引"),
    DROP_INDEX("删除索引"),
    TRANSACTION("事务");


    public String desc;

    SupportPower(String desc) {
        this.desc = desc;
    }

}
