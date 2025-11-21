package com.meiya.whalex.exception;

/**
 * 系统内部异常状态码定义
 *
 * @author 黄河森
 * @date 2019/9/16
 * @project whale-cloud-platformX
 */
public enum  ExceptionCode {

    /**
     * 组件服务无注解异常
     */
    INIT_DB_MODULE_EXCEPTION(100001, "ClassName: [%s] 缺少 @DbService 注解!"),
    /**
     * 组件参数转换工具类无注解异常
     */
    INIT_DB_PARAM_UTIL_EXCEPTION(100002, "ClassName: [%s] 缺少 @DbParamUtil 注解!"),
    DATABASE_NULL_EXCEPTION(100003, "未获取到数据库连接信息!"),
    TABLE_NULL_EXCEPTION(100004, "未获取到表配置信息!"),
    CONNECT_NULL_EXCEPTION(100005, "未获取到数据库链接对象!"),
    CONNECT_CACHE_KEY_NULL_EXCEPTION(100006, "未获取到数据库链接缓存KEY信息!"),
    INIT_DB_HELPER_UTIL_EXCEPTION(100007, "ClassName: [%s] 缺少 @DbHelper 注解!"),
    PARAM_NULL_EXCEPTION(100008, "操作参数转换实体为空!"),
    REFLECT_PARAM_MISMATCH(100009, "反射调用组件方法入参数量不匹配!"),
    DB_INFO_NULL_EXCEPTION(100010, "组件信息为空!"),
    DB_PARAM_ERROR_EXCEPTION(100011, "参数错误!"),
    DB_PARAM_ERROR_DESC_EXCEPTION(100011, "参数错误! 原因: [%s]"),
    CURRENT_CACHE_NOT_MULTI_LEVEL_CACHE(100012, "当前缓存 area: [%s] cacheName: [%s] 不为二级缓存!"),
    CLEAR_LOCAL_CACHE_EXCEPTION(100013, "当前缓存 area: [%s] cacheName: [%s] 清除本地缓存操作失败, 原因: [%s]!"),
    DATE_TYPE_TABLE_NULL_EXCEPTION(100014, "当前数据库表为分表类型，并且无法解析出当前查询所匹配的分表名称, msg: [%s]!"),
    METHOD_NOT_IMPLEMENTED_EXCEPTION(100015, "当前方法未实现, 目标方法: [%s]!"),
    DATE_TYPE_TABLE_TIME_NULL_EXCEPTION(100016, "当前请求数据库表为分表, 必须携带日期, 目标方法: [%s]!"),
    INIT_DB_SERVICE_NOT_FOUND_MODULE_EXCEPTION(100017, "当前组件 [%s] 从容器中获取不到对应组件的依赖服务 [%s] !"),
    DATE_TYPE_TABLE_PARSE_TIME_EXCEPTION(100018, "解析当前请求分表日期，获取对应表名失败!"),
    COUNT_FAILED_EXCEPTION(100019, "周期性物理表统计数量存在部分或者全部周期表失败!"),
    DB_QUERY_EXCEPTION(100020, "组件查询失败, [%s]!"),
    LINK_DATABASE_EXCEPTION(100021, "组件连接失败!, [%s]!"),
    CREATE_DB_CONNECT_EXCEPTION(100022, "创建数据库连接对象失败!"),
    CREATE_TABLE_EXISTS_EXCEPTION(100023, "请求创建的表已经存在!"),
    DATE_TYPE_NO_SET_EXCEPTION(100024, "当前数据库表为分表类型，并且未设置分表类型值!"),
    CREATE_TABLE_EXCEPTION(100025, "创建表操作执行失败, 失败的表: [%s]!"),
    DROP_TABLE_MISS_EXCEPTION(100026, "删除表操作失败, 被删除表不存在!"),
    DROP_TABLE_EXCEPTION(100027, "删除表操作失败, 失败的表: [%s]!"),
    NOT_FOUND_CERTIFICATE(100028, "找不到证书, keyId: [%s]!"),
    GET_CERTIFICATE_EXCEPTION(100029, "获取认证证书失败, keyId: [%s]!"),
    CERTIFICATE_LOGIN_EXCEPTION(100030, "证书登录异常, keyId: [%s]!"),
    NOT_FOUND_TABLE_EXCEPTION(100031, "检索的目标表[%s]不存在!"),
    DATE_TYPE_TABLE_TIME_STAR_GREATER_END_EXCEPTION(100032, "分表日期时间错误，开始时间大于结束时间!"),
    NOT_FOUND_DB_MODULE_SERVICE(100033, "未获取到当前组件的 DbModuleService 对象!"),
    NOT_FOUND_DB_MODULE_CONFIG_HELPER(100034, "未获取到当前组件的 AbstractDbModuleConfigHelper 对象!"),
    NOT_FOUND_DB_MODULE_PARAM_UTIL(100035, "未获取到当前组件的 AbstractDbModuleParamUtil 对象!"),
    ADD_DOC_EXCEPTION(100036, "添加文档失败, msg: [%s]!"),
    ADD_ROUTE_DOC_EXCEPTION(100044, "添加文档失败!路由字段 [%s] 在插入文档中不存在或者不是String类型!"),
    GET_TABLES_EXCEPTION(100037, "获取数据库表失败!"),
    NOT_FOUND_CURSOR(100038, "根据 cursorId 未获取到对应的游标信息，可能游标已经失效，请重新获取! dbId: [%s] cursorId: [%s]"),
    NOT_FOUNF_VERTEX(100039, "未获取到创建图的边的顶点！"),
    CREATE_GRAPH_EXCEPTION(100040, "Titan创建图失败！"),
    QUERY_GRAPH_EXCEPTION(100041, "当前查询类型未实现 rel：[%s]!"),
    DELETE_GRAPH_VERTEX_OR_EDGE_EXCEPTION(100042, "Titan图顶点或边删除失败！原因: [%s]!"),
    UPDATE_GRAPH_VERTEX_OR_EDGE_EXCEPTION(100043, "Titan图顶点或边更新失败！原因: [%s]!"),
    AGG_OP_NOT_SUPPORT_EXCEPTION(100044, "当前聚合操作 [%s] 不支持!"),
    TRANSACTION_NULL_EXCEPTION(100045, "未获取到事务管理器对象!"),
    TRANSACTION_ID_NULL_EXCEPTION(100046, "事务id不能为空!"),
    TRANSACTION_ID_EXIST_EXCEPTION(100047, "事务已开启!"),
    SQL_NULL_EXCEPTION(100048, "sql不能为空!"),
    UPSERT_PARAM_EXCEPTION(100049, "saveOrUpdate 操作参数异常! 原因：[%s]!"),
    CONNECT_FAILURE_EXCEPTION(100050, "组件连接测试失败!"),
    HDFS_MERGE_EXCEPTION(100051, "存在目录小文件合并异常, 异常信息: [%s]!"),
    HDFS_MERGE_FILE_TYPE_EXCEPTION(100052, "目前仅支持 [%s] 类型文件合并!"),
    HDFS_NOT_EXISTS_EXCEPTION(100053, "文件或目录不存在：[%s]!"),
    ;

    private int code;

    private String msg;

    ExceptionCode(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    /**
     * 根据状态码获取信息
     * @param code
     * @return
     */
    public static ExceptionCode findExceptionCodeEnum(int code) {
        ExceptionCode result = null;
        for (ExceptionCode exceptionCode : ExceptionCode.values()) {
            if (exceptionCode.getCode() == code) {
                result =  exceptionCode;
                break;
            }
        }
        return result;
    }
}
