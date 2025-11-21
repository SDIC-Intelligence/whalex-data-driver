package com.meiya.whalex.graph.module;

import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.graph.entity.GraphResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * 图数据库组件 Gremlin 查询接口
 *
 * @author 黄河森
 * @date 2022/12/27
 * @package com.meiya.whalex.graph.module
 * @project whalex-data-driver
 */
public interface GraphModuleService {

    /**
     * Gremlin 查询接口
     *
     * @param databaseSetting 数据库配置
     * @param tableSetting
     * @param gremlin
     * @return
     * @throws Exception
     */
    GraphResultSet submitGremlin(DatabaseSetting databaseSetting, TableSetting tableSetting, String gremlin) throws Exception;

    /**
     * 异步 Gremlin 查询接口
     *
     * @param databaseSetting
     * @param tableSetting
     * @param gremlin
     * @return
     * @throws Exception
     */
    Future<GraphResultSet> submitAsyncGremlin(DatabaseSetting databaseSetting, TableSetting tableSetting, String gremlin) throws Exception;

    /**
     * Gremlin 查询接口
     *
     * @param databaseSetting 数据库配置
     * @param tableSetting
     * @param gremlin
     * @param params
     * @return
     * @throws Exception
     */
    GraphResultSet submitGremlin(DatabaseSetting databaseSetting, TableSetting tableSetting, String gremlin, Map<String, Object> params) throws Exception;

    /**
     * 异步 Gremlin 查询接口
     *
     * @param databaseSetting
     * @param tableSetting
     * @param gremlin
     * @param params
     * @return
     * @throws Exception
     */
    Future<GraphResultSet> submitAsyncGremlin(DatabaseSetting databaseSetting, TableSetting tableSetting, String gremlin, Map<String, Object> params) throws Exception;

    /**
     * Gremlin 查询接口
     *
     * @param databaseSetting 数据库配置
     * @param tableSetting
     * @param traversal
     * @return
     * @throws Exception
     */
    GraphResultSet submitGremlin(DatabaseSetting databaseSetting, TableSetting tableSetting, Traversal traversal) throws Exception;

    /**
     * 异步 Gremlin 查询接口
     *
     * @param databaseSetting
     * @param tableSetting
     * @param traversal
     * @return
     * @throws Exception
     */
    Future<GraphResultSet> submitAsyncGremlin(DatabaseSetting databaseSetting, TableSetting tableSetting, Traversal traversal) throws Exception;


}
