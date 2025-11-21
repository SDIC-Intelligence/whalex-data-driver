package com.meiya.whalex.starter;

import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.business.service.DatabaseConfService;
import com.meiya.whalex.business.service.TableConfService;
import com.meiya.whalex.db.module.AbstractDbModuleBaseService;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.thread.entity.DbThreadBaseConfig;
import com.meiya.whalex.util.DbBeanManagerUtil;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.io.ScanJarClassUtil;
import com.meiya.whalex.util.io.YamlParser;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.util.*;

/**
 * 启动装载 数据库组件 对象到 组件管理容器中
 *
 * 在程序启动的 main 函数中调用 此类 init 方法。
 * 自动初始化依赖组件抽象层jar
 *
 * @author 黄河森
 * @date 2019/9/14
 * @project whale-cloud-platformX
 */
@Slf4j
public class InitDbModuleStartupRunner {

    /**
     * 记录全局是否已经执行初始化
     */
    private volatile static boolean initOver = false;

    /**
     * 是否执行资源目录配置初始化
     */
    private volatile static boolean isInitResourceConf = true;

    /**
     * 初始化入口
     * 脱离资源目录使用方式
     *
     * @throws Exception
     */
    public static void init() throws Exception {
        init(null, null);
    }

    /**
     * 初始化入口
     *
     * @throws Exception
     */
    public static void init(DatabaseConfService databaseConfService, TableConfService tableConfService) throws Exception {
        init(databaseConfService, tableConfService, null);
    }

    /**
     * 初始化入口，指定组件配置
     *
     * @param databaseConfService
     * @param tableConfService
     * @param dbThreadBaseConfigMap
     * @throws Exception
     */
    public static void init(DatabaseConfService databaseConfService, TableConfService tableConfService, Map<String, DbThreadBaseConfig> dbThreadBaseConfigMap) throws Exception {
        synchronized (InitDbModuleStartupRunner.class) {
            if (initOver) {
                if (!isInitResourceConf && databaseConfService != null && tableConfService != null) {
                    // 如果第一次初始化是非加载数据库的，那么在次调用初始化时如果存在加载数据库动作则更新 helper
                    log.info("全局环境已经初始化完成，重新执行加载数据库配置链接对象!");
                    reloadDbHelper(databaseConfService, tableConfService);
                    isInitResourceConf = false;
                } else {
                    log.warn("全局环境已经初始化完成，请勿重复调用初始化!");
                }
                return;
            }
            initOver = true;
            System.out.println("\n" +
                    "  _____       _______   _____  _____  _______      ________ _____  \n" +
                    " |  __ \\   /\\|__   __| |  __ \\|  __ \\|_   _\\ \\    / /  ____|  __ \\ \n" +
                    " | |  | | /  \\  | |    | |  | | |__) | | |  \\ \\  / /| |__  | |__) |\n" +
                    " | |  | |/ /\\ \\ | |    | |  | |  _  /  | |   \\ \\/ / |  __| |  _  / \n" +
                    " | |__| / ____ \\| |    | |__| | | \\ \\ _| |_   \\  /  | |____| | \\ \\ \n" +
                    " |_____/_/    \\_\\_|    |_____/|_|  \\_\\_____|   \\/   |______|_|  \\_\\");
            System.out.println("                                                     " + InitDbModuleStartupRunner.class.getPackage().getImplementationVersion() + " ");
            System.out.println("\n");
            log.info("开始执行数据库组件相关服务装载: ...");
            try {
                if (databaseConfService == null && tableConfService == null) {
                    isInitResourceConf = false;
                }
                Map<Class<? extends Annotation>, Set<Class<?>>> classSetMap = scanAnnotation();
                initDbThreadConfig(dbThreadBaseConfigMap);
                initParamUtil(classSetMap.get(DbParamUtil.class));
                initDbHelper(classSetMap.get(DbHelper.class), databaseConfService, tableConfService);
                initDbService(classSetMap.get(DbService.class));
                log.info("数据库组件相关服务装载结束!");
            } catch (Exception e) {
                log.error("数据库组件相关服务装载失败!", e);
                throw e;
            }
        }
    }

    public static void reloadDbHelper(DatabaseConfService databaseConfService, TableConfService tableConfService) throws Exception {
        Map<String, AbstractDbModuleConfigHelper> dbConfHelperCache = DbBeanManagerUtil.getDbConfHelperCache();
        for (AbstractDbModuleConfigHelper configHelper : dbConfHelperCache.values()) {
            configHelper.setDatabaseConfService(databaseConfService);
            configHelper.setTableConfService(tableConfService);
            configHelper.loadResConfig();
        }
    }

    /**
     * 扫描抽象层组件注解
     */
    public static Map<Class<? extends Annotation>, Set<Class<?>>> scanAnnotation() throws IOException {
        List<String> classPathConfig = new ArrayList<>();
        String resourceNameTem = "whalex-db-config-%s.yml";
        DbResourceEnum[] values = DbResourceEnum.values();
        for (DbResourceEnum dbResourceEnum : values) {
            String resourceName = String.format(resourceNameTem, dbResourceEnum.name());
            classPathConfig.addAll(getClassPathConfig(resourceName));
        }
        return ScanJarClassUtil.scanClassesByAnnotations(classPathConfig, Arrays.asList(DbHelper.class, DbParamUtil.class, DbService.class));
    }

    /**
     * 获取指定抽象层组件实现JAR中的资源配置文件
     *
     * @param resourceName
     * @return
     */
    public static List<String> getClassPathConfig(String resourceName) throws IOException {
        List<String> classNameList = new ArrayList<>();
        Enumeration<URL> resources = ScanJarClassUtil.class.getClassLoader().getResources(resourceName);
        while (resources.hasMoreElements()) {
            URL url = resources.nextElement();
            InputStream inputStream = url.openStream();
            Map<String, Map<String, String>> configMap = YamlParser.parserYaml(inputStream, Map.class);
            if (MapUtil.isNotEmpty(configMap)) {
                classNameList.addAll(configMap.get("whalex").values());
            }
        }
        return classNameList;
    }

    /**
     * 初始化组件库信息管理工具
     *
     * @throws Exception
     */
    private static void initDbHelper(Set<Class<?>> paramUtilClassSet, DatabaseConfService databaseConfService, TableConfService tableConfService) throws Exception {
        // 装载 组件参数转换工具类
        log.info("开始装载组件库信息管理工具:");
        for (Class aClass : paramUtilClassSet) {
            log.info(aClass.getName());
            DbHelper annotation = (DbHelper) aClass.getAnnotation(DbHelper.class);
            if (annotation == null) {
                throw new BusinessException(ExceptionCode.INIT_DB_HELPER_UTIL_EXCEPTION, aClass.getName());
            }
            DbResourceEnum dbResourceEnum = annotation.dbType();
            CloudVendorsEnum cloudVendorsEnum = annotation.cloudVendors();
            DbVersionEnum dbVersionEnum = annotation.version();
            AbstractDbModuleConfigHelper helper = (AbstractDbModuleConfigHelper) aClass.newInstance();
            helper.setDbResourceEnum(dbResourceEnum);
            helper.setCloudVendorsEnum(cloudVendorsEnum);
            helper.setDbVersionEnum(dbVersionEnum);
            helper.setDatabaseConfService(databaseConfService);
            helper.setTableConfService(tableConfService);
            helper.setThreadConfig(DbBeanManagerUtil.getBean(dbResourceEnum, dbVersionEnum, cloudVendorsEnum, DbThreadBaseConfig.class));
            DbBeanManagerUtil.initBean(dbResourceEnum, dbVersionEnum, cloudVendorsEnum, helper);
            helper.init(isInitResourceConf);
        }
        log.info("装载组件库信息管理工具结束!");
    }

    /**
     * 初始化参数转换工具
     */
    private static void initParamUtil(Set<Class<?>> paramUtilClassSet) throws Exception {
        // 装载 组件参数转换工具类
        log.info("开始装载组件参数工具类:");
        for (Class aClass : paramUtilClassSet) {
            log.info(aClass.getName());
            DbParamUtil annotation = (DbParamUtil) aClass.getAnnotation(DbParamUtil.class);
            if (annotation == null) {
                throw new BusinessException(ExceptionCode.INIT_DB_PARAM_UTIL_EXCEPTION, aClass.getName());
            }
            DbResourceEnum dbResourceEnum = annotation.dbType();
            CloudVendorsEnum cloudVendors = annotation.cloudVendors();
            DbVersionEnum version = annotation.version();
            DbBeanManagerUtil.initBean(dbResourceEnum, version, cloudVendors, (AbstractDbModuleParamUtil)aClass.newInstance());
        }
        log.info("装载组件参数工具类结束!");
    }

    /**
     * 初始化组件服务
     *
     * @throws Exception
     */
    private static void initDbService(Set<Class<?>> classSet) throws Exception {
        // 装载 组件服务类
        log.info("开始装载组件服务类:");
        for (Class aClass : classSet) {
            log.info(aClass.getName());
            DbService annotation = (DbService) aClass.getAnnotation(DbService.class);
            if (annotation == null) {
                throw new BusinessException(ExceptionCode.INIT_DB_MODULE_EXCEPTION, aClass.getName());
            }
            DbResourceEnum dbResourceEnum = annotation.dbType();
            CloudVendorsEnum cloudVendorsEnum = annotation.cloudVendors();
            DbVersionEnum dbVersionEnum = annotation.version();
            AbstractDbModuleBaseService service = (AbstractDbModuleBaseService) aClass.newInstance();
            // 设置参数转换工具类
            AbstractDbModuleParamUtil moduleParamUtil = DbBeanManagerUtil.getBean(dbResourceEnum, dbVersionEnum, cloudVendorsEnum, AbstractDbModuleParamUtil.class);
            if (moduleParamUtil == null) {
                throw new BusinessException(ExceptionCode.INIT_DB_SERVICE_NOT_FOUND_MODULE_EXCEPTION, dbResourceEnum.name(), AbstractDbModuleParamUtil.class.getName());
            }
            service.setDbModuleParamUtil(moduleParamUtil);
            AbstractDbModuleConfigHelper moduleConfigHelper = DbBeanManagerUtil.getBean(dbResourceEnum, dbVersionEnum, cloudVendorsEnum, AbstractDbModuleConfigHelper.class);
            if (moduleConfigHelper == null) {
                throw new BusinessException(ExceptionCode.INIT_DB_SERVICE_NOT_FOUND_MODULE_EXCEPTION, dbResourceEnum.name(), AbstractDbModuleConfigHelper.class.getName());
            }
            service.setHelper(moduleConfigHelper);
            service.setDbThreadBaseConfig(DbBeanManagerUtil.getBean(dbResourceEnum, dbVersionEnum, cloudVendorsEnum, DbThreadBaseConfig.class));
            DbBeanManagerUtil.initBean(dbResourceEnum, dbVersionEnum, cloudVendorsEnum, service);
        }
        log.info("装载组件服务类结束!");
    }

    /**
     * 初始化组件线程配置类
     *
     * @throws Exception
     */
    private static void initDbThreadConfig(Map<String, DbThreadBaseConfig> dbThreadBaseConfigMap) throws Exception {
        log.info("开始装载组件线程配置类:");
        if (MapUtil.isEmpty(dbThreadBaseConfigMap)) {
            URL resource = Thread.currentThread().getContextClassLoader().getResource("dbThreadConfig.yml");
            if (resource == null) {
                log.warn("not found dbThreadConfig.yml");
                return;
            }
            InputStream inputStream = null;
            Map<String, Object> config;
            try {
                inputStream = resource.openStream();
                config = YamlParser.parserYaml(inputStream, Map.class);
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
            Map<String, Object> cloudConfig = (Map<String, Object>) config.get("db-service");
            cloudConfig.forEach((key, value) -> {
                Map<String, Map<String, Integer>> dbConfig = (Map<String, Map<String, Integer>>) value;
                dbConfig.forEach((k, v) -> {
                    DbResourceEnum dbResourceEnum = DbResourceEnum.valueOf(k);
                    if (dbResourceEnum.equals(DbResourceEnum.undefine)) {
                        log.warn("当前线程配置组件未知! dbName: [{}]", k);
                        return;
                    }
                    DbBeanManagerUtil.initBean(dbResourceEnum, JsonUtil.jsonStrToObject(JsonUtil.objectToStr(v), DbThreadBaseConfig.class));
                });
            });
        } else {
            dbThreadBaseConfigMap.forEach((key, value) -> {
                DbResourceEnum dbResourceEnum = DbResourceEnum.valueOf(key);
                if (dbResourceEnum.equals(DbResourceEnum.undefine)) {
                    log.warn("当前线程配置组件未知! dbName: [{}]", key);
                    return;
                }
                DbBeanManagerUtil.initBean(dbResourceEnum, value);
            });
        }

        log.info("装载组件线程配置类结束!");
    }

    /**
     * 资源回收
     */
    public static void destroy() {
        synchronized (InitDbModuleStartupRunner.class) {
            if (!initOver) {
                log.warn("全局环境还未执行初始化加载或已完成环境销毁，请勿调用销毁方法!");
                return;
            }
            Map<String, AbstractDbModuleConfigHelper> dbConfHelperCache = DbBeanManagerUtil.getDbConfHelperCache();
            if (MapUtil.isNotEmpty(dbConfHelperCache)) {
                dbConfHelperCache.forEach((dbType, dbConfHelper) -> {
                    log.info("finalize dbType: [{}] ing...", dbType);
                    try {
                        dbConfHelper.finalizeRes();
                        log.info("finalize dbType: [{}] success.", dbType);
                    } catch (Exception e) {
                        log.error("finalize dbType: [{}] fail.", dbType, e);
                    }
                });
            }
            DbBeanManagerUtil.destroy();
            initOver = false;
        }
    }

    /**
     * 刷新库配置
     *
     * @param bigDataResourceId
     * @param dbResourceEnum
     */
    @Deprecated
    public static void refreshDatabaseConfig(String bigDataResourceId, DbResourceEnum dbResourceEnum) {
        AbstractDbModuleConfigHelper helperBean = DbBeanManagerUtil.getHelperBean(dbResourceEnum);
        helperBean.refreshDatabaseCache(bigDataResourceId);
    }

    /**
     * 刷新库配置
     *
     * @param bigDataResourceId
     * @param dbResourceEnum
     */
    public static void refreshDatabaseConfig(String bigDataResourceId, DbResourceEnum dbResourceEnum, DbVersionEnum dbVersionEnum, CloudVendorsEnum cloudVendorsEnum) {
        AbstractDbModuleConfigHelper helperBean = DbBeanManagerUtil.getHelperBean(dbResourceEnum, dbVersionEnum, cloudVendorsEnum);
        helperBean.refreshDatabaseCache(bigDataResourceId);
    }

    /**
     * 刷新表配置
     *
     * @param dbId
     * @param dbResourceEnum
     */
    public static void refreshTableConfig(String dbId, DbResourceEnum dbResourceEnum, DbVersionEnum dbVersionEnum, CloudVendorsEnum cloudVendorsEnum) {
        AbstractDbModuleConfigHelper helperBean = DbBeanManagerUtil.getHelperBean(dbResourceEnum, dbVersionEnum, cloudVendorsEnum);
        helperBean.refreshTableCache(dbId);
    }

    /**
     * 刷新表配置
     *
     * @param dbId
     * @param dbResourceEnum
     */
    @Deprecated
    public static void refreshTableConfig(String dbId, DbResourceEnum dbResourceEnum) {
        AbstractDbModuleConfigHelper helperBean = DbBeanManagerUtil.getHelperBean(dbResourceEnum);
        helperBean.refreshTableCache(dbId);
    }

}
