package com.meiya.whalex.graph.driver;

import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.graph.entity.GraphResultSet;
import com.meiya.whalex.graph.module.AbstractGraphModuleBaseService;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.util.DbBeanManagerUtil;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * 图数据库连接器
 *
 * @author 黄河森
 * @date 2023/2/9
 * @package com.meiya.whalex.graph.driver
 * @project whalex-data-driver
 */
public class GraphClient implements Closeable {

    /**
     * 数据库配置
     */
    private DatabaseSetting databaseSetting;

    /**
     * 表配置
     */
    private TableSetting tableSetting;

    /**
     * 图数据库服务类
     */
    private AbstractGraphModuleBaseService service;

    GraphClient(DatabaseSetting databaseSetting) {
        this.databaseSetting = databaseSetting;
        this.tableSetting = TableSetting.builder().tableName("g").build();
        this.service = (AbstractGraphModuleBaseService) DbBeanManagerUtil.getServiceBean(DbResourceEnum.findDbResourceEnum(databaseSetting.getDbType()));
    }

    public AbstractGraphModuleBaseService getService() {
        return service;
    }

    /**
     * 提交 Gremlin 语法
     *
     * @param gremlin
     * @return
     * @throws Exception
     */
    public GraphResultSet submit(final String gremlin) throws Exception {
        return this.service.submitGremlin(this.databaseSetting, this.tableSetting, gremlin);
    }

    /**
     * 提交 Gremlin 语法
     *
     * @param gremlin
     * @return
     * @throws Exception
     */
    public GraphResultSet submit(final String gremlin, Map<String, Object> params) throws Exception {
        return this.service.submitGremlin(this.databaseSetting, this.tableSetting, gremlin, params);
    }

    /**
     * 提交 Gremlin 语法
     *
     * @param traversal
     * @return
     * @throws Exception
     */
    public GraphResultSet submit(final Traversal traversal) throws Exception {
        return this.service.submitGremlin(this.databaseSetting, this.tableSetting, traversal);
    }

    /**
     * 异步执行 Gremlin
     *
     * @param gremlin
     * @return
     * @throws Exception
     */
    public Future<GraphResultSet> submitAsync(final String gremlin) throws Exception {
        return this.service.submitAsyncGremlin(this.databaseSetting, this.tableSetting, gremlin);
    }

    /**
     * 异步执行 Gremlin
     *
     * @param gremlin
     * @return
     * @throws Exception
     */
    public Future<GraphResultSet> submitAsync(final String gremlin, Map<String, Object> params) throws Exception {
        return this.service.submitAsyncGremlin(this.databaseSetting, this.tableSetting, gremlin, params);
    }

    /**
     * 异步执行 Gremlin
     *
     * @param traversal
     * @return
     * @throws Exception
     */
    public Future<GraphResultSet> submitAsync(final Traversal traversal) throws Exception {
        return this.service.submitAsyncGremlin(this.databaseSetting, this.tableSetting, traversal);
    }

    /**
     * 关闭连接
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        this.service.getHelper().destroyDbConnect(databaseSetting, tableSetting);
    }
}
