package com.meiya.whalex.db.builder;

import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.util.JsonUtil;

/**
 * @author 黄河森
 * @date 2021/6/21
 * @project whalex-data-driver-back
 */
public class DatabaseConfigurationBuilder<D> {

    private DatabaseSetting databaseSetting;

    private DatabaseConfigurationBuilder() {
        this.databaseSetting = new DatabaseSetting();
    }

    public static DatabaseConfigurationBuilder builder() {
        return new DatabaseConfigurationBuilder();
    }

    public DatabaseConfigurationBuilder type(DbResourceEnum dbResourceEnum) {
        this.databaseSetting.setDbType(dbResourceEnum.getVal());
        if (DbResourceEnum.hive.equals(dbResourceEnum)) {
            this.databaseSetting.setConnTypeId("02");
        }
        return this;
    }

    public DatabaseConfigurationBuilder database(D databaseConfig) {
        this.databaseSetting.setConnSetting(JsonUtil.objectToStr(databaseConfig));
        return this;
    }

    public DatabaseConfigurationBuilder cloud(CloudVendorsEnum cloudVendorsEnum) {
        this.databaseSetting.setCloudCode(cloudVendorsEnum.getCode());
        return this;
    }

    public DatabaseConfigurationBuilder version(String version) {
        this.databaseSetting.setVersion(version);
        return this;
    }

    public DatabaseConfigurationBuilder tag(String tag) {
        this.databaseSetting.setTag(tag);
        return this;
    }

    public DatabaseSetting build() {
        return this.databaseSetting;
    }


}
