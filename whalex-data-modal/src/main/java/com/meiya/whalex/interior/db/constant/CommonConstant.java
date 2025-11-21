package com.meiya.whalex.interior.db.constant;

/**
 * 特殊字典常量定义
 *
 * @author 黄河森
 * @date 2019/9/10
 * @project whale-cloud-platformX
 */
public interface CommonConstant {

    /**
     * 分表字段
     */
    String BURST_ZONE = "burstZone";

    String PARTITIONS = "partitions";

    String DISTANCE = "distance";

    String LOC = "loc";

    String LAT = "lat";

    String LON = "lon";

    String COMMA = ",";

    String ROW_KEY = "rowKey";

    String BASE64_ROW_KEY = "base64RowKey";

    String HBASE_DEL_COLUMNS = "bigTableDelColumns";

    String MONGO_ID = "_id";

    String CONTENT = "content";

    // 收藏 数据集 参数
    String UID = "uid";

    //  頂點
    String VERTEX = "vertex";

    //  邊
    String EDGE = "edge";

    //  邊
    String PATH = "path";
    
    //  节点标签
    String LABEL = "label";

    // 深度
    String DEPTH = "depth";

    // 最大深度
    String MAX_HOPS = "maxHops";

    // 最小深度
    String MIN_HOPS = "minHops";

    //  关系标签
    String RELATIONSHIP_LABEL = "relationshipLabel";

    //  关系节点信息
    String RELATIONSHIP_INFO = "relationshipInfo";

    //  起始节点
    String BEGIN = "begin";

    //  终止节点
    String END = "end";

    //  别名
    String ALIAS = "alias";
    String RELATIONSHIP_ALIAS = "relationshipAlias";

    //  节点
    String NODE_INFO = "nodeInfo";

    //  查询字段
    String SELECT_FIELD = "selectField";

    //  追加属性
    String APPEND = "append";

    //  删除节点和关系
    String DETACH = "detach";

    //  delete
    String DELETE = "delete";

    //  removeLabel
    String REMOVE_LABEL = "removeLabel";

    String REMOVE_FIELD = "removeField";

}
