package com.meiya.whalex.filesystem.util.param;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import com.meiya.whalex.db.entity.AbstractDbHandler;
import com.meiya.whalex.db.entity.AbstractDbTableInfo;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.filesystem.entity.QueryFileTreeNode;
import com.meiya.whalex.filesystem.entity.UploadFileParam;

import java.util.Map;

/**
 * 分布式文件条件解析
 *
 * @author 黄河森
 * @date 2022/12/27
 * @package com.meiya.whalex.graph.util.helper
 * @project whalex-data-driver
 */
public abstract class AbstractFileSystemParserUtil<Q extends AbstractDbHandler, D extends AbstractDatabaseInfo,
        T extends AbstractDbTableInfo> extends AbstractDbModuleParamUtil<Q ,D ,T> {


    /**
     * 查询文件树条件解析
     *
     * @param queryFileTreeNode
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    public abstract Q queryFileTreeNodeParser(QueryFileTreeNode queryFileTreeNode, D databaseConf, T tableConf) throws Exception;

    public abstract Q uploadFileParser(UploadFileParam uploadFileParam, D database, T table);

    public abstract Q downFileParser(String path, D database, T table);
}
