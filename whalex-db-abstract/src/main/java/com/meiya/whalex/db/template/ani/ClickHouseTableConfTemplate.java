package com.meiya.whalex.db.template.ani;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * ClickHouse 数据库表模板
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
@Builder
@Data
@AllArgsConstructor
public class ClickHouseTableConfTemplate {

    private String tableName;

    private String engine;

    private Boolean openDistributed;

    private Boolean openReplica;

    private Map<String, String> engineParamMap;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public ClickHouseTableConfTemplate() {
    }

    public ClickHouseTableConfTemplate(String tableName) {
        this.tableName = tableName;
    }
}
