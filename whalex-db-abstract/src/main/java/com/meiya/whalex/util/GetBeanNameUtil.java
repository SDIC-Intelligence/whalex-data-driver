package com.meiya.whalex.util;

import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;

/**
 * 获取各类实例BEAN名称
 *
 * @author 黄河森
 * @date 2019/9/13
 * @project whale-cloud-platformX
 */
public class GetBeanNameUtil {

    /**
     * 获取配置类初始化BEAN名称
     *
     * @return
     */
    public static String getConfigBeanName() {
        return "dbThreadConfig";
    }

    /**
     * 获取服务类初始化BEAN名称
     * @param dbResourceEnum
     * @return
     */
    @Deprecated
    public static String getDbServiceBeanName(DbResourceEnum dbResourceEnum) {
        return dbResourceEnum.name();
    }

    /**
     * 获取服务参数工具初始化BEAN名称
     * @param dbResourceEnum
     * @return
     */
    @Deprecated
    public static String getDbParamUtilBeanName(DbResourceEnum dbResourceEnum) {

        return dbResourceEnum.name();
    }

    /**
     * 获取服务数据源信息工具初始化BEAN名称
     * @param dbResourceEnum
     * @return
     */
    @Deprecated
    public static String getDbHelperBeanName(DbResourceEnum dbResourceEnum) {
        return dbResourceEnum.name();
    }

    /**
     * 获取服务类初始化BEAN名称
     * @param dbResourceEnum
     * @return
     */
    public static String getDbServiceBeanName(DbResourceEnum dbResourceEnum, DbVersionEnum versionEnum, CloudVendorsEnum cloudVendorsEnum) {
        return dbResourceEnum.name() + versionEnum.getVersion() + cloudVendorsEnum.name() + "Service";
    }

    /**
     * 获取服务参数工具初始化BEAN名称
     * @param dbResourceEnum
     * @return
     */
    public static String getDbParamUtilBeanName(DbResourceEnum dbResourceEnum, DbVersionEnum versionEnum, CloudVendorsEnum cloudVendorsEnum) {
        return dbResourceEnum.name() + versionEnum.getVersion() + cloudVendorsEnum.name() + "ParamUtil";
    }

    /**
     * 获取服务数据源信息工具初始化BEAN名称
     * @param dbResourceEnum
     * @return
     */
    public static String getDbHelperBeanName(DbResourceEnum dbResourceEnum, DbVersionEnum versionEnum, CloudVendorsEnum cloudVendorsEnum) {
        return dbResourceEnum.name() + versionEnum.getVersion() + cloudVendorsEnum.name() + "Helper";
    }
}
