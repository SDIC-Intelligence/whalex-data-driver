package com.meiya.whalex.db.entity.table.infomaration;

import cn.hutool.core.map.MapBuilder;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 黄河森
 * @date 2023/11/14
 * @package com.meiya.whalex.db.entity.table.infomaration
 * @project whalex-data-driver
 * @description KafkaTableInformation
 */
@Data
@NoArgsConstructor
public class KafkaTableInformation  extends TableInformation<Object>{

    public KafkaTableInformation(String tableName, Integer shards, Integer replicas, String distributionKey, String partitionKey, Map<String, Map<String, Object>> partitionInfos, Map<String, Map<Integer, Long>> groups) {
        super(tableName, shards, replicas, distributionKey, partitionKey, MapBuilder.create(new HashMap<String, Object>()).put("partitionInfos", partitionInfos).put("consumerGroups", groups).build());
    }

    @JsonIgnore
    public Map<String, Map<Integer, Long>> getConsumerGroups() {
        return (Map<String, Map<Integer, Long>>) this.extendMeta.get("consumerGroups");
    }

    @JsonIgnore
    public Map<String, Map<String, Object>> getPartitionInfos() {
        return (Map<String, Map<String, Object>>) this.extendMeta.get("partitionInfos");
    }

}
