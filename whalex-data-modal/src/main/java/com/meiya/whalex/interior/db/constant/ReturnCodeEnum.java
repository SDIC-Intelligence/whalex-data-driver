package com.meiya.whalex.interior.db.constant;

/**
 * 响应编码
 *
 * @author 黄河森
 * @date 2019/9/29
 * @project whale-cloud-platformX
 */
public enum ReturnCodeEnum {

    /**
     * 具体异常提示信息，请自定义
     */
    CODE_SUCCESS(0, "请求成功"),
    CODE_SUCCESS_QUERY_RESULT_NULL(0, "请求成功，无符合条件记录"),
    CODE_COUNT_NOT_FOUND_TABLE_WARN(0, "统计目标表不存在，返回数据量为0"),
    CODE_SYSTEM_ERROR(1001, "系统异常"),
    CODE_TIMEOUT_ERROR(1002, "操作超时"),
    CODE_REQUEST_PARAM_ERROR(1003, "请求参数异常"),
    CODE_WHERE_PARAM_NO_SEARCH(1004, "检索条件中 [%s] 字段不可用于检索"),
    CODE_RESOURCE_ID_NOT_FOUND(1005, "无此资源编码"),
    CODE_TABLE_NOT_FOUND_RESOURCE(1006, "此table无对应资源编码"),
    CODE_DATASET_NOT_FOUND_RESOURCE(1007, "此dataSet和dataSource无对应资源编码"),
    CODE_WHERE_PARAM_NOT_RIGHT(1008, "查询条件中有关键字无权查询"),
    CODE_RESOURCE_NOT_FOUND_ITEM_MAPPING(1009, "对应资源编码未找到标准编码与字段缩写的映射关系"),
    CODE_ITEM_CODE_NOT_FOUND_COLUMN(1010, "标准编码与字段缩写的映射关系中存在没有缩写映射的标准编码 [%s]"),
    CODE_SERVER_TYPE_NOT_FOUND(1011, "请求的服务类型不存在"),
    CODE_SEARCH_TYPE_NOT_FOUND(1012, "请求的查询类型参数不正确"),
    CODE_DB_NOT_FOUND(1014, "未匹配到符合条件的组件信息"),
    CODE_NOT_SEARCH_SERVER(1015, "非查询服务类型不支持默认查询"),
    CODE_TRANSMIT_ERROR(1016, "异地请求转发失败"),
    CODE_USER_CERTIFICATE_ERROR(1017, "用户的证件号码为空,请检查用户信息!"),
    CODE_MISS_ROLE(1018, "当前无角色参数"),
    CODE_ROLE_NOT_FOUND(1019, "当前角色不存在"),
    CODE_USER_NAME_ERROR(1020, "用户的真实名称为空, 请检查用户信息!"),
    CODE_CLOUD_SERVER_NOT_FOUND(1021, "资源挂载的云服务不存在, 云服务类型[%s]"),
    CODE_RESOURCE_NOT_FOUND_DB(1022, "指定资源不存在指定的部署定义!"),
    CODE_REJECT_ERROR(1023, "当前操作请求被拒绝，请求过多，请稍后重试"),
    CODE_TEST_CONNECT_FAIL(1024, "数据库链接测试失败, msg: %s"),
    CODE_CREATE_TABLE_FAIL(1025, "数据库建表操作失败, msg: %s"),
    CODE_CREATE_INDEX_FAIL(1026, "数据库建表索引失败, msg: %s"),
    CODE_NOT_IMPL_ERROR(1027, "当前组件请求方法暂未实现"),
    CODE_NO_HANDLER_ERROR(1028, "请求地址为无效地址"),
    CODE_METHOD_NOT_SUPPORT(1029, "目标方法不支持当前请求方式"),
    CODE_TYPE_NOT_ACCEPTABLE(1030, "请求头报文格式不正常"),
    CODE_DROP_TABLE_FAIL(1031, "数据库删表操作失败"),
    CODE_DROP_INDEX_FAIL(1032, "数据库删除索引操作失败, msg: %s"),
    CODE_MONITOR_DB_FAIL(1033, "数据库状态监控查询失败"),
    CODE_BUSINESS_ERROR(1034, "业务逻辑异常"),
    CODE_DATASET_NOT_FOUND(1035, "无此数据集"),
    CODE_COUNT_ERROR(1036, "组件统计数据量异常"),
    CODE_EXIST_OPERATION_FAIL(1037, "存在部分组件执行操作失败"),
    CODE_QUERY_ITEM_VERSION_FAIL(1038, "查询字段版本号必须指定单一字段"),
    CODE_BUSINESS_FAIL(1039, "业务处理全部或部分失败"),
    CODE_DATA_SOURCE_RESOURCE_NOT_FOUND(1040, "溯源资源标识符未匹配到挂载的组件"),
    CODE_SY_RESOURCE_NOT_FOUND(1041, "溯源资源目录未找到"),
    CODE_SEARCH_SY_RESOURCE_FAIL(1042, "溯源资源检索失败, msg: %s"),
    CODE_SEARCH_SY_RESOURCE_NULL(1043, "溯源资源检索不到匹配的记录"),
    CODE_QUERY_CURSOR_DBID_NULL(1044, "游标查询需要指定dbId或dbType"),
    CODE_QUERY_CURSOR_NOT_FOUND(1045, "游标已过期，请重新查询!"),
    CODE_QUERY_CURSOR_HOLD_LOCK(1046, "当前游标ID已经被持有并正在进行查询，请等待游标查询结束之后再重新尝试!"),
    CODE_QUERY_CURSOR_BATCH_SIZE_MAX(1047, "当前游标操作批量数大于 10000，请重新设置，单次拉取最大10000!"),
    CODE_QUERY_BLACK_LIST(1048, "当前查询库表为黑名单库表, 暂停查询服务!"),
    CODE_QUERY_FUSING(1049, "当前查询资源频繁超时, 暂停该资源服务!"),
    CODE_QUERY_RES_POOL_FULL(1050, "当前可使用服务资源已经耗尽, 请稍后重试!"),
    CODE_QUERY_CURSOR_SCHEDULE_NOT_FOUND(1051, "游标滚动记录以不存在，请重新拉取!"),
    CODE_ALTER_TABLE_FAIL(1052, "数据库修改表操作失败: %s!"),
    CODE_DATASOURCE_NOT_FOUND(1053, "无此数据源"),
    CODE_STATISTIC_FILTER_DB_TYPE(1054, "资源数据量统计过滤组件类型dbType：%s!"),
    CODE_STATISTIC_FILTER_BIG_RESOURCE_ID(1055, "资源数据量统计过滤数据库bigResourceId：%s!"),
    CODE_STATISTIC_FILTER_DB_ID(1056, "资源数据量统计过滤数据库表dbId：%s!"),
    CODE_ZERO_TRUST_LINE_AUTH(1057, "零信任行数据鉴权异常, 异常信息: %s"),
    CODE_NOT_FOUND_METHOD(1058, "%s不存在方法%s"),
    // MQ 消费异常信息
    CODE_THREAD_FULL_ERROR(2001, "任务繁忙,稍后再试"),
    CODE_MQ_TIMEOUT_ERROR(2002, "操作等待超时"),
    CODE_SEQID_NULL_WARN(2003, "流水号id为空"),
    CODE_API_ERROR(2004, "DbAsyncMethodEnum定义方法不符规范"),
    CODE_API_CLASS_NOT_FOUND(2005, "DbAsyncMethodEnum定义方法找不到对应Class"),
    CODE_API_METHOD_NOT_FOUND(2006, "DbAsyncMethodEnum定义方法找不到对应Method"),
    CODE_API_METHOD_INVOKE_ERROR(2007, "DbAsyncMethodEnum定义方法执行时出现异常"),
    CODE_REBACK_FUTURE_ERROR(2008, "阻塞监听反馈消息异常"),
    CODE_REBACK_RESULT_ERROR(2009, "反馈消息异常补充"),
    // http请求异常
    CODE_HTTP_IO_FAIL(3001, "HTTP请求异常"),
    CODE_HTTP_STATUS_FAIL(3002, "HTTP状态码异常"),
    CODE_HTTP_EXECUTE_FAIL(3003, "HTTP请求逻辑异常"),
    CODE_HTTP_RESULT_EMPTY(3004, "HTTP请求无结果返回"),
    CODE_INVALID_TRANSACTION_ID(3005, "无效的事务id"),
    CODE_TRANSACTION_ID_IS_NULL(3006, "事务id为空"),

    /**
     * 特定组件的异常编码从9开头
     */
    CODE_FILE_SYSTEM_NO_IMPL(9001, "当前操作组件未实现分布式文件系统类型的接口"),
    ;

    private int code;
    private String msg;

    ReturnCodeEnum(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }

    public String format() {
        return String.format("%s#%s", code, msg);
    }
}
