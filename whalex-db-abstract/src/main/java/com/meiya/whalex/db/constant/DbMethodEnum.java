package com.meiya.whalex.db.constant;

import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateDatabaseParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.DropDatabaseParamCondition;
import com.meiya.whalex.interior.db.operation.in.MergeDataParamCondition;
import com.meiya.whalex.interior.db.operation.in.PublishMessage;
import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
import com.meiya.whalex.interior.db.operation.in.SubscribeMessage;
import com.meiya.whalex.interior.db.operation.in.UpdateDatabaseParamCondition;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;

import java.io.InputStream;
import java.util.function.Consumer;

/**
 * 组件方法枚举
 *
 * @author 黄河森
 * @date 2019/9/12
 * @project whale-cloud-platformX
 */
public enum  DbMethodEnum {

    /**
     * 分页查询
     */
    QUERY_PAGE("queryListForPage", DbHandleEntity.class, QueryParamCondition.class),
    /**
     * 单记录查询
     */
    QUERY_ONE("queryOne", DbHandleEntity.class, QueryParamCondition.class),
    /**
     * 集合查询
     */
    QUERY_LIST("queryList", DbHandleEntity.class, QueryParamCondition.class),
    /**
     * 统计
     */
    STATISTICAl_LINE("statisticalLine", DbHandleEntity.class, QueryParamCondition.class),
    /**
     * 数据新增
     */
    INSERT("insert", DbHandleEntity.class, AddParamCondition.class),
    /**
     * 数据批量新增
     */
    INSERT_BATCH("insertBatch", DbHandleEntity.class, AddParamCondition.class),
    /**
     * 数据更新
     */
    UPDATE("update", DbHandleEntity.class, UpdateParamCondition.class),
    /**
     * 数据删除
     */
    DELETE("delete", DbHandleEntity.class, DelParamCondition.class),
    /**
     * 索引查询
     */
    QUERY_INDEXES("getIndexes", DbHandleEntity.class),
    /**
     * 索引创建
     */
    CREATE_INDEXES("createIndex", DbHandleEntity.class, IndexParamCondition.class),
    /**
     * 连接测试
     */
    TEST_CONNECTION("testConnection", DbHandleEntity.class),

    /**
     * 是否开始changelog功能
     */
    CHANGELOG("changelog", DbHandleEntity.class),

    /**
     * 组件能力
     */
    SUPPORT_POWER_LIST("supportList"),

    /**
     * 获取库中所有的表信息
     */
    QUERY_LIST_TABLE("queryListTable", DbHandleEntity.class, QueryTablesCondition.class),
    /**
     * 创建表
     */
    CREATE_TABLE("createTable", DbHandleEntity.class, CreateTableParamCondition.class),
    /**
     * 修改表
     */
    ALTER_TABLE("alterTable", DbHandleEntity.class, AlterTableParamCondition.class),
    /**
     * 删除表
     */
    DROP_TABLE("dropTable", DbHandleEntity.class, DropTableParamCondition.class),


    /**
     * 清空表
     */
    EMPTY_TABLE("emptyTable", DbHandleEntity.class, EmptyTableParamCondition.class),
    /**
     * 删除索引
     */
    DELETE_INDEX("deleteIndex", DbHandleEntity.class, IndexParamCondition.class),
    /**
     * 数据库状态监控
     */
    MONITOR_STATUS("monitorStatus", DbHandleEntity.class),
    /**
     * 获取表结构信息
     */
    QUERY_TABLE_SCHEMA("queryTableSchema", DbHandleEntity.class),
    /**
     * 游标查询
     */
    QUERY_CURSOR("queryCursor", DbHandleEntity.class, QueryParamCondition.class, Consumer.class),
    /**
     * 新增或更新
     */
    UPSERT("saveOrUpdate", DbHandleEntity.class, UpsertParamCondition.class),
    /**
     * 获取库schema列表
     */
    QUERY_DATABASE_SCHEMA("queryDatabaseSchema", DbHandleEntity.class),
    /**
     * 判断表是否存在
     */
    TABLE_EXISTS("tableExists", DbHandleEntity.class),
    /**
     * 获取表相关信息
     */
    TABLE_INFORMATION("queryTableInformation", DbHandleEntity.class),
    /**
     * 获取数据库列表
     */
    QUERY_LIST_DATABASE("queryListDatabase", DbHandleEntity.class, QueryDatabasesCondition.class),
    /**
     * 获取数据库列表
     */
    DATABASE_INFORMATION("queryDatabaseInformation", DbHandleEntity.class),

    /**
     * 提交事务
     */
    COMMIT_TRANSACTION("commit"),

    /**
     * 回滚事务
     */
    ROLLBACK_TRANSACTION("rollback"),

    /**
     * 组件定制化操作，需要解析表配置
     */
    CUSTOMIZE_OPERATION_ON_TABLE("customizeOnTable"),
    /**
     * 组件定制化操作，需要解析库配置不需要解析表配置
     */
    CUSTOMIZE_OPERATION_ON_DATABASE("customizeOnDatabase"),

    /**
     * 执行sql
     */
    EXECUTE_SQL("executeSql"),

    /**
     * 执行raw statement
     */
    EXECUTE_RAW_STATEMENT("executeRawStatement"),

    /**
     * 合并数据
     */
    MERGE_DATA("mergeData", DbHandleEntity.class, MergeDataParamCondition.class),

    /**
     * 创建数据库
     */
    CREATE_DATABASE("createDatabase", DbHandleEntity.class, CreateDatabaseParamCondition.class),

    /**
     * 更新数据库
     */
    UPDATE_DATABASE("updateDatabase", DbHandleEntity.class, UpdateDatabaseParamCondition.class),


    /**
     * 删除数据库
     */
    DROP_DATABASE("dropDatabase", DbHandleEntity.class, DropDatabaseParamCondition.class),

    QUERY_PARTITION("queryPartitionInformation", DbHandleEntity.class),

    QUERY_PARTITION_V2("queryPartitionInformationV2", DbHandleEntity.class),

    SUBSCRIBE("subscribe", DbHandleEntity.class, SubscribeMessage.class),

    PUBLISH("publish", DbHandleEntity.class, PublishMessage.class),

    FILESYSTEM_LL("ll"),

    FILESYSTEM_DOWN_FILE("downFile"),
    FILESYSTEM_UPLOAD_FILE("uploadFile"),
    ;
    /**
     * 方法名称
     */
    private String method;

    /**
     * 方法参数
     */
    private Class<?>[] parameterTypes;

    DbMethodEnum(String method, Class<?>... parameterTypes) {
        this.method = method;
        this.parameterTypes = parameterTypes;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Class<?>[] getParameterTypes() {
        return parameterTypes;
    }

    public void setParameterTypes(Class<?>[] parameterTypes) {
        this.parameterTypes = parameterTypes;
    }
}
