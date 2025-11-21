package com.meiya.whalex.db.entity.graph;

/**
 * @author chenjp
 * @date 2/25/2021
 */
public interface GraphBaseConstant {

    String ALIAS = "alias";
    String V = "vertex";
    String E = "edge";
    String AS = "as";
    String SELECT = "select";
    String NULL = "null";
    String EQ = "eq";
    String NEQ = "neq";
    String LT = "lt";
    String LTE = "lte";
    String GT = "gt";
    String GTE = "gte";
    String INSIDE = "insider";
    String OUTSIDE = "outside";
    String BETWEEN = "between";
    String WITHIN = "within";
    String WITHOUT = "without";
    String INCR = "incr";
    String DECR = "decr";
    String SHUFFLE = "shuffle";
    String UNDER = "__";
    String LAB = "lab";

    /** 基本 **/
    String ID = "id";
    String LABEL = "label";
    String PROPERTIES = "properties";
    String KEY = "key";
    String VALUE = "value";
    String VALUE_MAP = "valueMap";
    String VALUES = "values";

    /** 边的遍历操作 **/
    String IN = "in";
    String OUT = "out";
    String BOTH = "both";
    String OUT_E = "outE";
    String IN_E = "inE";
    String BOTH_E = "bothE";
    String OUT_V = "outV";
    String IN_V = "inV";
    String BOTH_V = "bothV";
    String OTHER_V = "otherV";

    /** has条件过滤 **/
    String HAS_LABEL = "hasLabel";
    String HAS_ID = "hasId";
    String HAS = "has";
    String HAS_KEY = "hasKey";
    String HAS_VALUE = "hasValue";
    String HAS_NOT = "hasNot";

    /** 图查询返回结果数限制 **/
    String COUNT = "count";
    String RANGE = "range";
    String LIMIT = "limit";
    String TAIL = "tail";
    String SKIP = "skip";

    /** 查询路径 **/
    String PATH = "path";
    String SIMPLE_PATH = "simplePath";
    String CYCLIC_PATH = "cyclicPath";

    /** 循环操作 **/
    String REPEAT = "repeat";
    String TIMES = "times";
    String UNTIL = "until";
    String EMIT = "emit";
    String LOOPS = "loops";

    /** 查询结果排序 **/
    String ORDER = "order";
    String BY = "by";

    /** 数据分组与去重 **/
    String GROUP = "group";
    String GROUP_COUNT = "groupCount";
    String DEDUP = "dedup";

    /** 条件和过滤 **/
    String WHERE = "where";
    String FILTER = "filter";

    /** 逻辑运算 **/
    String IS = "is";
    String AND = "and";
    String OR = "or";
    String NOT = "not";

    /** 统计运算 **/
    String SUM = "sum";
    String MAX = "max";
    String MIN = "min";
    String MEAN = "mean";

    /** 数学运算 **/
    String MATH = "math";

    /** 路径选取与过滤 **/
    String AS_SELECT = "asSelect";
    String AS_WHERE = "asWhere";
    String AS_MATCH = "asMatch";
    String AS_DEDUP = "asDedup";

    /** 分支 **/
    String CHOOSE = "choose";
    String BRANCH = "branch";

    /** 合并 **/
    String COALESCE = "coalesce";
    String OPTIONAL = "optional";
    String UNION = "union";

    /** 结果聚集与展开 **/
    String AGGREGATE = "aggregate";
    String STORE = "store";
    String UNFOLD = "unfold";
    String FOLD = "fold";

    /** 模式匹配 **/
    String MATCH = "match";

    /** 随机过滤与注入 **/
    String SAMPLE = "sample";
    String COIN = "coin";
    String CONSTANT = "constant";
    String INJECT = "inject";

    /** 结果存取口袋 **/
    String SACK = "sack";

    /** 遍历栅栏 **/
    String BARRIER = "barrier";

    /** 局部操作 **/
    String LOCAL = "local";

    /** 遍历终止 **/
    String HAS_NEXT = "hasNext";
    String NEXT = "next";
    String TRY_NEXT = "tryNext";
    String TO_LIST = "toList";
    String TO_SET = "toSet";
    String TO_BULK_SET = "toBulkSet";
    String FILL = "fill";
    String ITERATE = "iterate";

    /** 转换操作 **/
    String MAP = "map";
    String FLATMAP = "flatMap";

    /** 附加操作sideEffect **/
    String SIDE_EFFECT = "sideEffect";

    /** 执行统计和分析 **/
    String PROFILE = "profile";
    String EXPLAIN = "explain";

}
