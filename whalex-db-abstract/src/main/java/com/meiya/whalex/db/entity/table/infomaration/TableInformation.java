package com.meiya.whalex.db.entity.table.infomaration;

import com.meiya.whalex.util.JsonUtil;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * 表信息查询结果封装
 *
 * @author 黄河森
 * @date 2021/9/10
 * @package com.meiya.whalex.db.entity
 * @project whalex-data-driver
 */
@Data
@Builder
public class TableInformation<E> {

    /**
     * 表名
     */
    protected String tableName;

    /**
     * 分片数
     */
    protected Integer shards;

    /**
     * 副本数
     */
    protected Integer replicas;

    /**
     * 分布键
     */
    protected String distributionKey;

    /**
     * 分区键
     */
    protected String partitionKey;

    /**
     * 扩展内容
     * 特定组件可能需要返回特定的内容
     */
    protected Map<String, E> extendMeta;

    public TableInformation() {
    }

    public TableInformation(String tableName, Integer shards, Integer replicas, String distributionKey, String partitionKey, Map<String, E> extendMeta) {
        this.tableName = tableName;
        this.shards = shards;
        this.replicas = replicas;
        this.distributionKey = distributionKey;
        this.partitionKey = partitionKey;
        this.extendMeta = extendMeta;
    }

    public Map<String, Object> toMap() {
        return JsonUtil.entityToMap(this);
    }
}
