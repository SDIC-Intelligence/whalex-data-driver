package com.meiya.whalex.db.entity.table.infomaration;

import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author 黄河森
 * @date 2023/11/14
 * @package com.meiya.whalex.db.entity.table.infomaration
 * @project whalex-data-driver
 * @description MysqlTableInformation
 */
@NoArgsConstructor
public class OracleTableInformation extends TableInformation<Object> {

    public OracleTableInformation(String tableName, Integer shards, Integer replicas, String distributionKey, String partitionKey, Map<String, Object> meta) {
        super(tableName, shards, replicas, distributionKey, partitionKey, meta);
    }

    //行、表类型、注释
    public String getTableComment() {
        return (String) extendMeta.get("COMMENTS");
    }

    public String getTableType() {
        Object partitioned = extendMeta.get("PARTITIONED");
        if("NO".equals(partitioned)) {
            return  "Normal";
        }
        return "Partitioned";
    }

    public Long getRowCount() {
        Object rowCount = extendMeta.get("NUM_ROWS");
        if(rowCount == null) return 0L;

        if(rowCount instanceof BigDecimal) {
            return ((BigDecimal) rowCount).longValue();
        }

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
