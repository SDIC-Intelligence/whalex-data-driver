package com.meiya.whalex.filesystem.module;

import com.meiya.whalex.db.constant.DbMethodEnum;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.module.AbstractDbModuleBaseService;
import com.meiya.whalex.db.stream.StreamIterator;
import com.meiya.whalex.filesystem.entity.FileTreeNode;
import com.meiya.whalex.filesystem.entity.QueryFileTreeNode;
import com.meiya.whalex.filesystem.entity.UploadFileParam;
import com.meiya.whalex.filesystem.util.param.AbstractFileSystemParserUtil;

/**
 * 分布式文件系统统一抽象接口
 *
 * @author 黄河森
 * @date 2023/9/1
 * @package com.meiya.whalex.filesystem.module
 * @project whalex-data-driver
 * @description DistributedFileSystemService
 */
public abstract class AbstractDistributedFileSystemBaseService <S,
        Q extends AbstractDbHandler,
        D extends AbstractDatabaseInfo,
        T extends AbstractDbTableInfo,
        C extends AbstractCursorCache> extends AbstractDbModuleBaseService<S, Q, D, T, C> implements DistributedFileSystemService {

    @Override
    public AbstractFileSystemParserUtil getDbModuleParamUtil() {
        return (AbstractFileSystemParserUtil) super.getDbModuleParamUtil();
    }

    @Override
    public FileTreeNode ll(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryFileTreeNode queryFileTreeNode) throws Exception {
        FileTreeNode fileTreeNode = interiorExecute(databaseSetting
                , tableSetting
                , queryFileTreeNode
                , DbMethodEnum.CUSTOMIZE_OPERATION_ON_TABLE
                , new DbModuleCallback<S, Q, D, T, FileTreeNode>() {
                    @Override
                    public FileTreeNode doWithExecute(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
                        return ll(connect, queryEntity, databaseConf, tableConf);
                    }
                }, new DbModuleParserCallback<QueryFileTreeNode, Q, D, T>() {
                    @Override
                    public Q parser(QueryFileTreeNode queryParams, D database, T table) throws Exception {
                        return (Q) getDbModuleParamUtil().queryFileTreeNodeParser(queryParams, database, table);
                    }
                });
        return fileTreeNode;
    }

    @Override
    public StreamIterator<byte[]> downFile(DatabaseSetting databaseSetting, TableSetting tableSetting, String path) throws Exception {
        StreamIterator<byte[]> streamIterator = interiorExecute(databaseSetting
                , tableSetting
                , path
                , DbMethodEnum.CUSTOMIZE_OPERATION_ON_TABLE
                , new DbModuleCallback<S, Q, D, T, StreamIterator<byte[]>>() {
                    @Override
                    public StreamIterator<byte[]> doWithExecute(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
                        return downFile(connect, queryEntity, databaseConf, tableConf);
                    }
                }, new DbModuleParserCallback<String, Q, D, T>() {
                    @Override
                    public Q parser(String path, D database, T table) throws Exception {
                        return (Q) getDbModuleParamUtil().downFileParser(path, database, table);
                    }
                });
        return streamIterator;
    }

    @Override
    public PageResult uploadFile(DatabaseSetting databaseSetting, TableSetting tableSetting, UploadFileParam uploadFileParam) throws Exception {
        PageResult pageResult = interiorExecute(databaseSetting
                , tableSetting
                , uploadFileParam
                , DbMethodEnum.CUSTOMIZE_OPERATION_ON_TABLE
                , new DbModuleCallback<S, Q, D, T, PageResult>() {
                    @Override
                    public PageResult doWithExecute(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
                        return uploadFile(connect, queryEntity, databaseConf, tableConf);
                    }
                }, new DbModuleParserCallback<UploadFileParam, Q, D, T>() {
                    @Override
                    public Q parser(UploadFileParam uploadFileParam, D database, T table) throws Exception {
                        return (Q) getDbModuleParamUtil().uploadFileParser(uploadFileParam, database, table);
                    }
                });
        return pageResult;
    }

    protected abstract PageResult uploadFile(S connect, Q queryEntity, D database, T table) throws Exception;

    /**
     * 检索文件树
     *
     * @param connect
     * @param database
     * @param table
     * @param queryEntity
     * @return
     * @throws Exception
     */
    protected abstract FileTreeNode ll(S connect, Q queryEntity, D database, T table) throws Exception;

    protected abstract StreamIterator<byte[]> downFile(S connect, Q queryEntity, D database, T table) throws Exception;


}
