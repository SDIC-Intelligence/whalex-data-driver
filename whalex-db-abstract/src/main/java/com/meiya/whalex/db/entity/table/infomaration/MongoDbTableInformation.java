package com.meiya.whalex.db.entity.table.infomaration;

import lombok.NoArgsConstructor;

import java.util.Map;

@NoArgsConstructor
public class MongoDbTableInformation extends TableInformation<Object> {

    public MongoDbTableInformation(String tableName, Integer shards, Integer replicas, String distributionKey, String partitionKey, Map<String, Object> extendMeta) {
        super(tableName, shards, replicas, distributionKey, partitionKey, extendMeta);
    }

    public Boolean isSharded() {
        Boolean sharded = (Boolean) extendMeta.get("sharded");
        if (sharded == null) {
            return false;
        }
        return sharded;
    }

    public Boolean isCapped() {
        Boolean capped = (Boolean) extendMeta.get("capped");
        if (capped == null) {
            return false;
        }
        return capped;
    }

    public Long getRows() {
        return Long.valueOf(String.valueOf(extendMeta.get("rows")));
    }

    public Long getSize() {
        return Long.valueOf(String.valueOf(extendMeta.get("size")));
    }

    public Long getStorageSize() {
        return Long.valueOf(String.valueOf(extendMeta.get("storageSize")));
    }

    public Double getAvgObjSize() {
        return Double.valueOf(String.valueOf(extendMeta.get("avgObjSize")));
    }

    public Long getNChunks() {
        return Long.valueOf(String.valueOf(extendMeta.get("nchunks")));
    }

    public Long getMaxSize() {
        return Long.valueOf(String.valueOf(extendMeta.get("maxSize")));
    }

    public Map<String, Object> getShardInfo() {
        return (Map<String, Object>) extendMeta.get("shardInfo");
    }
}
