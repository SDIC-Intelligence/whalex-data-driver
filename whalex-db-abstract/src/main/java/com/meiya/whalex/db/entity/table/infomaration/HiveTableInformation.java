package com.meiya.whalex.db.entity.table.infomaration;

import lombok.NoArgsConstructor;

import java.util.Map;

@NoArgsConstructor
public class HiveTableInformation extends TableInformation<Object>{

    public HiveTableInformation(String tableName, Integer shards, Integer replicas, String distributionKey, String partitionKey, Map<String, Object> extendMeta) {
        super(tableName, shards, replicas, distributionKey, partitionKey, extendMeta);
    }

    //表类型
    public String getTableType(){
        return (String) extendMeta.get("TableType");
    }

    //存储位置
    public String getLocation() {
        return (String) extendMeta.get("Location");
    }

    //输入格式
    public String getInputFormat() {
        return (String) extendMeta.get("InputFormat");
    }

    //输出格式
    public String getOutputFormat() {
        return (String) extendMeta.get("OutputFormat");
    }

    //注释
    public String getComment() {
        return (String) extendMeta.get("comment");
    }

}
