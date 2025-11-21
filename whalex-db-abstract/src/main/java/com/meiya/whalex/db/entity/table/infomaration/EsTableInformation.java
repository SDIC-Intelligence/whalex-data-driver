package com.meiya.whalex.db.entity.table.infomaration;

import lombok.NoArgsConstructor;

import java.util.Map;

@NoArgsConstructor
public class EsTableInformation extends TableInformation<Object>{

    public EsTableInformation(String tableName, Integer shards, Integer replicas, String distributionKey, String partitionKey, Map<String, Object> extendMeta) {
        super(tableName, shards, replicas, distributionKey, partitionKey, extendMeta);
    }

    //行、分片数、副本数、滚动索引（策略名称、滚动别名、滚动策略）
    public String getRolloverAlias(){
        return (String) extendMeta.get("rolloverAlias");
    }

    public String getRolloverName() {
        return (String) extendMeta.get("rolloverName");
    }


    public String getRolloverPolicyMaxAge() {
        Map<String, Object> rolloverPolicy = (Map<String, Object>) extendMeta.get("rolloverPolicy");
        if(rolloverPolicy == null) {
            return null;
        }
        Object max_age = rolloverPolicy.get("max_age");
        if(max_age != null) return max_age.toString();
        return null;
    }


    public String getRolloverPolicyMaxSize() {
        Map<String, Object> rolloverPolicy = (Map<String, Object>) extendMeta.get("rolloverPolicy");
        if(rolloverPolicy == null) {
            return null;
        }
        Object max_size = rolloverPolicy.get("max_size");
        if(max_size != null) return max_size.toString();
        return null;
    }

    public String getRolloverPolicyMaxDocs() {
        Map<String, Object> rolloverPolicy = (Map<String, Object>) extendMeta.get("rolloverPolicy");
        if(rolloverPolicy == null) {
            return null;
        }
        Object max_docs = rolloverPolicy.get("max_docs");
        if(max_docs != null) max_docs.toString();
        return null;
    }


    public Long getRowCount() {
        Object rowCount = extendMeta.get("rowCount");
        if(rowCount == null) return 0L;
        Long aLong = Long.valueOf(rowCount.toString());
        return aLong;
    }

}
