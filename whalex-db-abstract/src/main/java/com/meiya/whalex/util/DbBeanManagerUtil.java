package com.meiya.whalex.util;

import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.DbHandleEntity;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.module.AbstractDbModuleBaseService;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.thread.entity.DbThreadBaseConfig;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 组件管理容器
 *
 * @author 黄河森
 * @date 2019/12/12
 * @project whaleX-common
 */
public class DbBeanManagerUtil {

    /**
     * 缓存组件配置相关的服务
     */
    private static final Map<String, AbstractDbModuleConfigHelper> DB_CONF_HELPER_CACHE = new ConcurrentHashMap<>();

    /**
     * 缓存组件参数转换服务
     */
    private static final Map<String, AbstractDbModuleParamUtil> DB_PARAM_UTIL_CACHE = new ConcurrentHashMap<>();

    /**
     * 缓存组件服务
     */
    private static final Map<String, DbModuleService> DB_SERVICE_CACHE = new ConcurrentHashMap<>();

    /**
     * 缓存组件线程池配置
     */
    private static final Map<String, DbThreadBaseConfig> DB_THREAD_CONF_CACHE = new ConcurrentHashMap<>();

    /**
     * 初始化组件配置相关BEAN
     *
     * @param dbResourceEnum
     * @param helper
     */
    public static void initBean(DbResourceEnum dbResourceEnum, DbVersionEnum versionEnum, CloudVendorsEnum cloudVendorsEnum, AbstractDbModuleConfigHelper helper) {
        String dbHelperBeanName = GetBeanNameUtil.getDbHelperBeanName(dbResourceEnum, versionEnum, cloudVendorsEnum);
        DB_CONF_HELPER_CACHE.put(dbHelperBeanName, helper);
    }

    /**
     * 初始化组件参数转换相关BEAN
     *
     * @param
     * @param paramUtil
     */
    public static void initBean(DbResourceEnum dbResourceEnum, DbVersionEnum versionEnum, CloudVendorsEnum cloudVendorsEnum, AbstractDbModuleParamUtil paramUtil) {
        String dbParamUtilBeanName = GetBeanNameUtil.getDbParamUtilBeanName(dbResourceEnum, versionEnum, cloudVendorsEnum);
        DB_PARAM_UTIL_CACHE.put(dbParamUtilBeanName, paramUtil);
    }

    /**
     * 初始化组件服务相关BEAN
     *
     * @param dbResourceEnum
     * @param service
     */
    public static void initBean(DbResourceEnum dbResourceEnum, DbVersionEnum versionEnum, CloudVendorsEnum cloudVendorsEnum, DbModuleService service) {
        String dbServiceBeanName = GetBeanNameUtil.getDbServiceBeanName(dbResourceEnum, versionEnum, cloudVendorsEnum);
        DB_SERVICE_CACHE.put(dbServiceBeanName, service);
    }

    /**
     * 初始化组件线程配置相关BEAN
     *
     * @param dbResourceEnum
     * @param config
     */
    public static void initBean(DbResourceEnum dbResourceEnum, DbThreadBaseConfig config) {
        DB_THREAD_CONF_CACHE.put(dbResourceEnum.name(), config);
    }

    /**
     * 获取 bean
     * @param beanName
     * @param tClass
     * @param <T>
     * @return
     */
    @Deprecated
    public static <T> T getBean(String beanName, Class<T> tClass) {
        if (tClass.equals(AbstractDbModuleBaseService.class) || tClass.equals(DbModuleService.class)) {
            DbModuleService dbModuleService = null;
            Set<String> keySet = DB_SERVICE_CACHE.keySet();
            for(String key : keySet) {
                if(key.startsWith(beanName)) {
                    dbModuleService = DB_SERVICE_CACHE.get(key);
                }
            }
            if (dbModuleService == null) {
                throw new BusinessException(ExceptionCode.NOT_FOUND_DB_MODULE_SERVICE);
            }
            return (T) dbModuleService;
        }else if (tClass.equals(AbstractDbModuleParamUtil.class)){
            AbstractDbModuleParamUtil abstractDbModuleParamUtil = null;
            Set<String> keySet = DB_PARAM_UTIL_CACHE.keySet();
            for(String key : keySet) {
                if(key.startsWith(beanName)) {
                    abstractDbModuleParamUtil = DB_PARAM_UTIL_CACHE.get(key);
                }
            }
            if (abstractDbModuleParamUtil == null) {
                throw new BusinessException(ExceptionCode.NOT_FOUND_DB_MODULE_PARAM_UTIL);
            }
            return (T) abstractDbModuleParamUtil;
        } else if (tClass.equals(AbstractDbModuleConfigHelper.class)) {
            AbstractDbModuleConfigHelper abstractDbModuleConfigHelper = null;
            Set<String> keySet = DB_CONF_HELPER_CACHE.keySet();
            for(String key : keySet) {
                if(key.startsWith(beanName)) {
                    abstractDbModuleConfigHelper = DB_CONF_HELPER_CACHE.get(key);
                }
            }
            if (abstractDbModuleConfigHelper == null) {
                throw new BusinessException(ExceptionCode.NOT_FOUND_DB_MODULE_CONFIG_HELPER);
            }
            return (T) abstractDbModuleConfigHelper;
        } else if (tClass.equals(DbThreadBaseConfig.class)) {
            DbThreadBaseConfig dbThreadBaseConfig = DB_THREAD_CONF_CACHE.get(beanName);
            if (dbThreadBaseConfig == null) {
                dbThreadBaseConfig = new DbThreadBaseConfig();
                DB_THREAD_CONF_CACHE.put(beanName, dbThreadBaseConfig);
            }
            return (T) DB_THREAD_CONF_CACHE.get(beanName);
        } else {
            throw new BusinessException("未获取到当前指定的bean对象!");
        }
    }

    /**
     * 获取 bean
     *
     * @param
     * @param tClass
     * @param <T>
     * @return
     */
    public static <T> T getBean(DbResourceEnum dbResourceEnum, DbVersionEnum versionEnum, CloudVendorsEnum cloudVendorsEnum, Class<T> tClass) {
        if (tClass.equals(AbstractDbModuleBaseService.class) || tClass.equals(DbModuleService.class)) {
            String beanName = GetBeanNameUtil.getDbServiceBeanName(dbResourceEnum, versionEnum, cloudVendorsEnum);
            DbModuleService dbModuleService = DB_SERVICE_CACHE.get(beanName);
            if (dbModuleService == null) {
                throw new BusinessException(ExceptionCode.NOT_FOUND_DB_MODULE_SERVICE);
            }
            return (T) dbModuleService;
        } else if (tClass.equals(AbstractDbModuleParamUtil.class)){
            String beanName = GetBeanNameUtil.getDbParamUtilBeanName(dbResourceEnum, versionEnum, cloudVendorsEnum);
            AbstractDbModuleParamUtil abstractDbModuleParamUtil = DB_PARAM_UTIL_CACHE.get(beanName);
            if (abstractDbModuleParamUtil == null) {
                throw new BusinessException(ExceptionCode.NOT_FOUND_DB_MODULE_PARAM_UTIL);
            }
            return (T) abstractDbModuleParamUtil;
        } else if (tClass.equals(AbstractDbModuleConfigHelper.class)) {
            String beanName = GetBeanNameUtil.getDbHelperBeanName(dbResourceEnum, versionEnum, cloudVendorsEnum);
            AbstractDbModuleConfigHelper abstractDbModuleConfigHelper = DB_CONF_HELPER_CACHE.get(beanName);
            if (abstractDbModuleConfigHelper == null) {
                throw new BusinessException(ExceptionCode.NOT_FOUND_DB_MODULE_CONFIG_HELPER);
            }
            return (T) abstractDbModuleConfigHelper;
        } else if (tClass.equals(DbThreadBaseConfig.class)) {
            String beanName = dbResourceEnum.name();
            DbThreadBaseConfig dbThreadBaseConfig = DB_THREAD_CONF_CACHE.get(beanName);
            if (dbThreadBaseConfig == null) {
                dbThreadBaseConfig = new DbThreadBaseConfig();
                DB_THREAD_CONF_CACHE.put(beanName, dbThreadBaseConfig);
            }
            return (T) DB_THREAD_CONF_CACHE.get(beanName);
        } else {
            throw new BusinessException("未获取到当前指定的bean对象!");
        }
    }

    public static Map<String, DbThreadBaseConfig> getDbConfCache() {
        return DB_THREAD_CONF_CACHE;
    }

    public static Map<String, AbstractDbModuleConfigHelper> getDbConfHelperCache() {
        return DB_CONF_HELPER_CACHE;
    }

    public static DbModuleService getServiceBean(DbResourceEnum dbResourceEnum) {
        return DbBeanManagerUtil.getBean(GetBeanNameUtil.getDbServiceBeanName(dbResourceEnum), DbModuleService.class);
    }

    public static DbModuleService getServiceBean(DbResourceEnum dbResourceEnum, DbVersionEnum version, CloudVendorsEnum cloudVendorsEnum) {
        return DbBeanManagerUtil.getBean(dbResourceEnum, version, cloudVendorsEnum, DbModuleService.class);
    }

    public static AbstractDbModuleConfigHelper getHelperBean(DbResourceEnum dbResourceEnum, DbVersionEnum version, CloudVendorsEnum cloudVendorsEnum) {
        return DbBeanManagerUtil.getBean(dbResourceEnum, version, cloudVendorsEnum, AbstractDbModuleConfigHelper.class);
    }

    public static AbstractDbModuleConfigHelper getHelperBean(DbResourceEnum dbResourceEnum) {
        return DbBeanManagerUtil.getBean(GetBeanNameUtil.getDbHelperBeanName(dbResourceEnum), AbstractDbModuleConfigHelper.class);
    }

    public static AbstractDbModuleParamUtil getParamUtilBean(DbResourceEnum dbResourceEnum, DbVersionEnum version, CloudVendorsEnum cloudVendorsEnum) {
        return DbBeanManagerUtil.getBean(dbResourceEnum, version, cloudVendorsEnum, AbstractDbModuleParamUtil.class);
    }

    public static AbstractDbModuleParamUtil getParamUtilBean(DbResourceEnum dbResourceEnum) {
        return DbBeanManagerUtil.getBean(GetBeanNameUtil.getDbParamUtilBeanName(dbResourceEnum), AbstractDbModuleParamUtil.class);
    }

    @Deprecated
    public static <T> T getDataSource(DbResourceEnum dbResourceEnum, DbVersionEnum version, CloudVendorsEnum cloudVendorsEnum, DbHandleEntity dbHandleEntity, Class<T> clazz) throws Exception {
        return (T) getHelperBean(dbResourceEnum, version, cloudVendorsEnum).getDbConnect(dbHandleEntity);
    }

    @Deprecated
    public static <T> T getDataSource(DbResourceEnum dbResourceEnum, DbHandleEntity dbHandleEntity, Class<T> clazz) throws Exception {
        return (T) getHelperBean(dbResourceEnum).getDbConnect(dbHandleEntity);
    }

    public static <T> T getDataSource(DbResourceEnum dbResourceEnum, DbVersionEnum version, CloudVendorsEnum cloudVendorsEnum, DatabaseSetting databaseSetting, TableSetting tableSetting, Class<T> clazz) throws Exception {
        return (T) getHelperBean(dbResourceEnum, version, cloudVendorsEnum).getDbConnect(databaseSetting, tableSetting);
    }

    public static <T> T getDataSource(DbResourceEnum dbResourceEnum, DatabaseSetting databaseSetting, TableSetting tableSetting, Class<T> clazz) throws Exception {
        return (T) getHelperBean(dbResourceEnum).getDbConnect(databaseSetting, tableSetting);
    }

    public static void destroy() {
        DB_CONF_HELPER_CACHE.clear();
        DB_PARAM_UTIL_CACHE.clear();
        DB_SERVICE_CACHE.clear();
        DB_THREAD_CONF_CACHE.clear();
    }
}
