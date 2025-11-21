package com.meiya.whalex.db.util.param.impl.ani;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Filter;
import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BaseMySqlDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseMySqlTableInfo;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.operation.in.*;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * MySql 参数转换工具类
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseMySqlParamUtil<Q extends AniHandler, D extends BaseMySqlDatabaseInfo, T extends BaseMySqlTableInfo> extends AbstractDbModuleParamUtil<Q, D, T> {
    BaseMySqlParserUtil baseMySqlParserUtil = new BaseMySqlParserUtil();

    protected BaseMySqlParserUtil getBaseMysqlParserUtil() {
        return baseMySqlParserUtil;
    }

    @Override
    protected Q transitionListTableParam(QueryTablesCondition queryTablesCondition, D databaseConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniListTable aniListTable = new AniHandler.AniListTable();
        aniHandler.setListTable(aniListTable);
        String tableMatch = queryTablesCondition.getTableMatch();
        tableMatch = StringUtils.replaceEach(tableMatch, new String[]{"*", "?"}, new String[]{"%", "_"});
        aniListTable.setTableMatch(tableMatch);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, D databaseConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniListDatabase aniListDatabase = new AniHandler.AniListDatabase();
        aniHandler.setListDatabase(aniListDatabase);
        String databaseMatch = queryDatabasesCondition.getDatabaseMatch();
        databaseMatch = StringUtils.replaceEach(databaseMatch, new String[]{"*", "?"}, new String[]{"%", "_"});
        aniListDatabase.setDatabaseMatch(databaseMatch);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniCreateTable(getBaseMysqlParserUtil().parserCreateTableSql(createTableParamCondition, databaseConf, tableConf));
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, D databaseConf, T tableConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniAlterTable(getBaseMysqlParserUtil().parserAlterTableSql(alterTableParamCondition, databaseConf, tableConf));
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionCreateIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniCreateIndex aniCreateIndex = getBaseMysqlParserUtil().parserCreateIndexSql(indexParamCondition, databaseConf, tableConf);
        aniHandler.setAniCreateIndex(aniCreateIndex);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionDropIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniDropIndex aniDropIndex = getBaseMysqlParserUtil().parserDropIndexSql(indexParamCondition, databaseConf, tableConf);
        aniHandler.setAniDropIndex(aniDropIndex);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionQueryParam(QueryParamCondition queryParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniQuery aniQuery;
        aniQuery = getBaseMysqlParserUtil().parserQuerySql(queryParamCondition, databaseConf, tableConf);
        aniQuery.setCount(queryParamCondition.isCountFlag());
        aniQuery.setBatchSize(queryParamCondition.getBatchSize());
        aniHandler.setAniQuery(aniQuery);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionUpdateParam(UpdateParamCondition updateParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniUpdate aniUpdate = getBaseMysqlParserUtil().parserUpdateSql(updateParamCondition, databaseConf, tableConf);
        aniHandler.setAniUpdate(aniUpdate);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionInsertParam(AddParamCondition addParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniInsert aniInsert = getBaseMysqlParserUtil().parserInsertSql(addParamCondition, databaseConf, tableConf);
        aniHandler.setAniInsert(aniInsert);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionDeleteParam(DelParamCondition delParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniDel(getBaseMysqlParserUtil().parserDelSql(delParamCondition, databaseConf, tableConf));
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionDropTableParam(DropTableParamCondition dropTableParamCondition, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniDropTable dropTable = new AniHandler.AniDropTable();
        dropTable.setIfExists(dropTableParamCondition.isIfExists());
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniDropTable(dropTable);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionEmptyTableParam(EmptyTableParamCondition emptyTableParamCondition, D databaseConf, T tableConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        return (Q) aniHandler;
    }

    @Override
    public Q transitionUpsertParam(UpsertParamCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniUpsert aniUpsert = getBaseMysqlParserUtil().parserUpsertSql(paramCondition, databaseConf, tableConf);
        // 组装对象
        AniHandler upsertHandler = new AniHandler();
        upsertHandler.setAniUpsert(aniUpsert);
        return (Q) upsertHandler;
    }

    @Override
    public Q transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniUpsertBatch aniUpsertBatch = getBaseMysqlParserUtil().parserUpsertBatchSql(paramCondition, databaseConf, tableConf);
        // 组装对象
        AniHandler upsertHandler = new AniHandler();
        upsertHandler.setAniUpsertBatch(aniUpsertBatch);
        return (Q) upsertHandler;
    }

    @Override
    protected Q transitionCreateDatabaseParam(CreateDatabaseParamCondition paramCondition, D dataConf) {
        AniHandler.AniCreateDatabase aniCreateDatabase = getBaseMysqlParserUtil().parserCreateDatabase(paramCondition, dataConf);
        // 组装对象
        AniHandler aniHandler = new AniHandler();
        aniHandler.setCreatedatabase(aniCreateDatabase);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionUpdateDatabaseParam(UpdateDatabaseParamCondition paramCondition, D dataConf) {
        AniHandler.AniUpdateDatabase aniUpdateDatabase = getBaseMysqlParserUtil().parserUpdateDatabase(paramCondition, dataConf);
        // 组装对象
        AniHandler aniHandler = new AniHandler();
        aniHandler.setUpdatedatabase(aniUpdateDatabase);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionDropDatabaseParam(DropDatabaseParamCondition paramCondition, D dataConf) {
        AniHandler.AniDropDatabase aniDropDatabase = getBaseMysqlParserUtil().parserDropDatabase(paramCondition, dataConf);
        // 组装对象
        AniHandler aniHandler = new AniHandler();
        aniHandler.setDropdatabase(aniDropDatabase);
        return (Q) aniHandler;
    }
}
