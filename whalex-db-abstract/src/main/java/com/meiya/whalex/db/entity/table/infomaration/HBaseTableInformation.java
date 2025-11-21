package com.meiya.whalex.db.entity.table.infomaration;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author 黄河森
 * @date 2023/11/14
 * @package com.meiya.whalex.db.entity.table.infomaration
 * @project whalex-data-driver
 * @description HBaseTableInformation
 */
@Data
@NoArgsConstructor
public class HBaseTableInformation extends TableInformation<Map<String, HBaseTableInformation.HBaseRegionInfo>>{


    public HBaseTableInformation(String tableName, Integer shards, Integer replicas, String distributionKey, String partitionKey, Map<String, Map<String, HBaseRegionInfo>> extendMeta) {
        super(tableName, shards, replicas, distributionKey, partitionKey, extendMeta);
    }

    public Map<String, HBaseRegionInfo> getRegionsInfo() {
        Map<String, HBaseRegionInfo> regions = extendMeta.get("regions");
        return regions;
    }

    @Data
    @Builder
    public static class HBaseRegionInfo {
        private String encodedName;
        private String regionName;
        private Long regionId;
        private Integer replicaId;
        private String startKey;
        private String endKey;

        public HBaseRegionInfo() {
        }

        public HBaseRegionInfo(String encodedName, String regionName, Long regionId, Integer replicaId, String startKey, String endKey) {
            this.encodedName = encodedName;
            this.regionName = regionName;
            this.regionId = regionId;
            this.replicaId = replicaId;
            this.startKey = startKey;
            this.endKey = endKey;
        }
    }

}
