package com.meiya.whalex.db.entity.table.infomaration;

import lombok.NoArgsConstructor;

import java.util.Map;

@NoArgsConstructor
public class PostGreTableInformation extends TableInformation<Object>{

    public PostGreTableInformation(String tableName, Integer shards, Integer replicas, String distributionKey, String partitionKey, Map<String, Object> extendMeta) {
        super(tableName, shards, replicas, distributionKey, partitionKey, extendMeta);
    }


    //行、表类型、注释
    public String getTableComment() {
        return (String) extendMeta.get("tableComment");
    }

    public String getTableType() {
        return (String) extendMeta.get("tableType");
    }

    public Long getRowCount() {
        Object rowCount = extendMeta.get("rowCount");
        if(rowCount == null) return 0L;
        if(rowCount instanceof Double) {
            return ((Double) rowCount).longValue();
        }
        if(rowCount instanceof Float) {
            return ((Float) rowCount).longValue();
        }
        Long aLong = Long.valueOf(rowCount.toString());
        return aLong;
    }

}
