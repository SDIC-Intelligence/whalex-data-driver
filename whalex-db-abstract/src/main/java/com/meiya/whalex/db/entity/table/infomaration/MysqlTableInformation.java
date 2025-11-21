package com.meiya.whalex.db.entity.table.infomaration;

import cn.hutool.core.date.DateUtil;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @author 黄河森
 * @date 2023/11/14
 * @package com.meiya.whalex.db.entity.table.infomaration
 * @project whalex-data-driver
 * @description MysqlTableInformation
 */
@NoArgsConstructor
public class MysqlTableInformation  extends TableInformation<Object> {

    public MysqlTableInformation(String tableName, Integer shards, Integer replicas, String distributionKey, String partitionKey, Map<String, Object> meta) {
        super(tableName, shards, replicas, distributionKey, partitionKey, meta);
    }

    public String getEngine() {
        return (String) this.extendMeta.get("engine");
    }

    public String getVersion() {
        return (String) this.extendMeta.get("version");
    }

    public String getRowFormat() {
        return (String) this.extendMeta.get("rowFormat");
    }

    public Long getRows() {
        return this.extendMeta.get("rows") == null ? null : Long.valueOf(String.valueOf(this.extendMeta.get("rows")));
    }

    public Long getAvgRowLength() {
        return this.extendMeta.get("avgRowLength") == null ? null : Long.valueOf(String.valueOf(this.extendMeta.get("avgRowLength")));
    }

    public Long getDataLength() {
        return this.extendMeta.get("avgRowLength") == null ? null : Long.valueOf(String.valueOf(this.extendMeta.get("avgRowLength")));
    }

    public Long getMaxDataLength() {
        return this.extendMeta.get("maxDataLength") == null ? null : Long.valueOf(String.valueOf(this.extendMeta.get("maxDataLength")));
    }

    public Long getIndexLength() {
        return this.extendMeta.get("indexLength") == null ? null : Long.valueOf(String.valueOf(this.extendMeta.get("indexLength")));
    }

    public Long getDataFree() {
        return this.extendMeta.get("dataFree") == null ? null : Long.valueOf(String.valueOf(this.extendMeta.get("dataFree")));
    }

    public Long getAutoIncrement() {
        return this.extendMeta.get("autoIncrement") == null ? null : Long.valueOf(String.valueOf(this.extendMeta.get("autoIncrement")));
    }

    public String getCreateTime() {
        Long createTime = (Long) this.extendMeta.get("createTime");
        if (createTime != null) {
            return DateUtil.format(DateUtil.date(createTime), "yyyy-MM-dd HH:mm:ss");
        } else {
            return null;
        }
    }

    public String getUpdateTime() {
        Long updateTime = (Long) this.extendMeta.get("updateTime");
        if (updateTime != null) {
            return DateUtil.format(DateUtil.date(updateTime), "yyyy-MM-dd HH:mm:ss");
        } else {
            return null;
        }
    }

    public String getCollation() {
        return (String) this.extendMeta.get("collation");
    }

    public String getComment() {
        return (String) this.extendMeta.get("comment");
    }

}
