package com.meiya.whalex.db.builder;

import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.util.JsonUtil;

/**
 * @author 黄河森
 * @date 2021/6/21
 * @project whalex-data-driver-back
 */
public class TableConfigurationBuilder<T> {

    private TableSetting tableSetting;

    private TableConfigurationBuilder() {
        this.tableSetting = new TableSetting();
    }

    public static TableConfigurationBuilder builder() {
        return new TableConfigurationBuilder();
    }

    public TableConfigurationBuilder tableName(String tableName) {
        this.tableSetting.setTableName(tableName);
        return this;
    }

    public TableConfigurationBuilder tableConfig(T tableConfig) {
        this.tableSetting.setTableJson(JsonUtil.objectToStr(tableConfig));
        return this;
    }

    public TableSetting build() {
        return this.tableSetting;
    }
}
