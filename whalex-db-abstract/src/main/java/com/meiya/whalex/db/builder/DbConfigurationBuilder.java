package com.meiya.whalex.db.builder;

import com.meiya.whalex.db.entity.DbHandleEntity;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.util.JsonUtil;

/**
 * 当前构造类已经废弃，请使用
 *
 * {@link com.meiya.whalex.db.builder.DatabaseConfigurationBuilder}
 * {@link com.meiya.whalex.db.builder.TableConfigurationBuilder}
 *
 * @author 黄河森
 * @date 2021/6/21
 * @project whalex-data-driver-back
 */
@Deprecated
public class DbConfigurationBuilder<D, T> {
    //--------------------------------------------------------------
    // 废弃 API
    //--------------------------------------------------------------

    private DbHandleEntity dbHandleEntity;

    private DbConfigurationBuilder() {
        this.dbHandleEntity = new DbHandleEntity();
    }
    @Deprecated
    public static DbConfigurationBuilder builder() {
        return new DbConfigurationBuilder();
    }
    @Deprecated
    public DbConfigurationBuilder type(DbResourceEnum dbResourceEnum) {
        this.dbHandleEntity.setDbType(dbResourceEnum.getVal());
        if (DbResourceEnum.hive.equals(dbResourceEnum)) {
            this.dbHandleEntity.setConnTypeId("02");
        }
        return this;
    }
    @Deprecated
    public DbConfigurationBuilder database(D databaseConfig) {
        this.dbHandleEntity.setConnSetting(JsonUtil.objectToStr(databaseConfig));
        return this;
    }
    @Deprecated
    public DbConfigurationBuilder tableName(String tableName) {
        this.dbHandleEntity.setTableName(tableName);
        return this;
    }
    @Deprecated
    public DbConfigurationBuilder tableConfig(T tableConfig) {
        this.dbHandleEntity.setTableJson(JsonUtil.objectToStr(tableConfig));
        return this;
    }
    @Deprecated
    public DbConfigurationBuilder cloud(CloudVendorsEnum cloudVendorsEnum) {
        this.dbHandleEntity.setCloudCode(cloudVendorsEnum.getCode());
        return this;
    }
    @Deprecated
    public DbConfigurationBuilder version(String version) {
        this.dbHandleEntity.setVersion(version);
        return this;
    }
    @Deprecated
    public DbHandleEntity build() {
        return this.dbHandleEntity;
    }


}
