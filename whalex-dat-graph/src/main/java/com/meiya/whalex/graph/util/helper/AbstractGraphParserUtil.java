package com.meiya.whalex.graph.util.helper;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import com.meiya.whalex.db.entity.AbstractDbHandler;
import com.meiya.whalex.db.entity.AbstractDbTableInfo;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.Map;

/**
 * 图数据库Gremlin语法解析
 *
 * @author 黄河森
 * @date 2022/12/27
 * @package com.meiya.whalex.graph.util.helper
 * @project whalex-data-driver
 */
public abstract class AbstractGraphParserUtil<Q extends AbstractDbHandler, D extends AbstractDatabaseInfo,
        T extends AbstractDbTableInfo> extends AbstractDbModuleParamUtil<Q ,D ,T> {


    /**
     * gremlin 语法解析
     *
     * @param gremlin
     * @param params
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    public abstract Q gremlinParser(String gremlin, Map<String, Object> params, D databaseConf, T tableConf) throws Exception;

    /**
     * gremlin 语法解析
     *
     * @param traversal
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    public abstract Q traversalParser(Traversal traversal, D databaseConf, T tableConf) throws Exception;

    /**
     * ql 语法解析
     *
     * @param ql
     * @param params
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    public abstract Q qlParser(String ql, Map<String, Object> params, D databaseConf, T tableConf) throws Exception;

}
