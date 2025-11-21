package com.meiya.whalex.graph.module;


import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.graph.entity.QlResult;

import java.util.Map;

/**
 * 图数据库组件 QL(CQL/GQL) 查询接口
 *
 * @author 黄河森
 * @date 2022/12/27
 * @package com.meiya.whalex.graph.module
 * @project whalex-data-driver
 */
public interface QlGraphModuleService {

    /**
     * ql 执行入口
     *
     * @param databaseSetting 数据库配置
     * @param tableSetting
     * @param ql
     * @return
     * @throws Exception
     */
    QlResult run(DatabaseSetting databaseSetting, TableSetting tableSetting, String ql) throws Exception;


    /**
     * ql 执行入口，带参数
     *
     * @param databaseSetting 数据库配置
     * @param tableSetting
     * @param ql
     * @param parameterMap
     * @return
     * @throws Exception
     */
    QlResult run(DatabaseSetting databaseSetting, TableSetting tableSetting, String ql, Map<String, Object> parameterMap) throws Exception;

}
