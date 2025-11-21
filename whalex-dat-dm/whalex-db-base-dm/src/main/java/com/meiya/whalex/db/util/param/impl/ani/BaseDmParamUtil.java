package com.meiya.whalex.db.util.param.impl.ani;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Filter;
import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BaseDmDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseDmTableInfo;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
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
 * DM 参数转换工具类
 *
 * @author 蔡荣桂
 * @date 2021/4/14
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseDmParamUtil<Q extends AniHandler, D extends BaseDmDatabaseInfo, T extends BaseDmTableInfo> extends AbstractDbModuleParamUtil<Q, D, T> {
    BaseDmParserUtil baseDmParserUtil = new BaseDmParserUtil();

    public BaseDmParserUtil getBaseDmParserUtil() {
        return baseDmParserUtil;
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
        aniHandler.setAniCreateTable(getBaseDmParserUtil().parserCreateTableSql(createTableParamCondition, databaseConf, tableConf));

        return (Q) aniHandler;
    }

    @Override
    protected Q transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, D databaseConf, T tableConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniAlterTable(getBaseDmParserUtil().parserAlterTableSql(alterTableParamCondition, databaseConf, tableConf));

        return (Q) aniHandler;

    }

    @Override
    protected Q transitionCreateIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniCreateIndex aniCreateIndex = getBaseDmParserUtil().parserCreateIndexSql(indexParamCondition, databaseConf, tableConf);
        aniHandler.setAniCreateIndex(aniCreateIndex);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionDropIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniDropIndex aniDropIndex = getBaseDmParserUtil().parserDropIndexSql(indexParamCondition, databaseConf, tableConf);
        aniHandler.setAniDropIndex(aniDropIndex);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionQueryParam(QueryParamCondition queryParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniQuery aniQuery;
        // 如果存在自定义SQL，则不需要转换
        String sql = queryParamCondition.getSql();
        if (StringUtils.isNotBlank(sql)) {
            aniQuery = new AniHandler.AniQuery();
            aniQuery.setSql(sql);
        } else {
            aniQuery = getBaseDmParserUtil().parserQuerySql(queryParamCondition, databaseConf, tableConf);
            aniQuery.setCount(queryParamCondition.isCountFlag());
            aniQuery.setBatchSize(queryParamCondition.getBatchSize());
        }
        aniHandler.setAniQuery(aniQuery);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionUpdateParam(UpdateParamCondition updateParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniUpdate aniUpdate = getBaseDmParserUtil().parserUpdateSql(updateParamCondition, databaseConf, tableConf);
        aniHandler.setAniUpdate(aniUpdate);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionInsertParam(AddParamCondition addParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniInsert aniInsert = getBaseDmParserUtil().parserInsertSql(addParamCondition, databaseConf, tableConf);
        aniHandler.setAniInsert(aniInsert);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionDeleteParam(DelParamCondition delParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniDel(getBaseDmParserUtil().parserDelSql(delParamCondition, databaseConf, tableConf));
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionDropTableParam(DropTableParamCondition dropTableParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) new AniHandler();
    }

    @Override
    public Q transitionUpsertParam(UpsertParamCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        // 组装对象
        AniHandler upsertHandler = new AniHandler();
        AniHandler.AniUpsert aniUpsert = getBaseDmParserUtil().parserUpsertSql(paramCondition, databaseConf, tableConf);
        upsertHandler.setAniUpsert(aniUpsert);
        return (Q) upsertHandler;
    }

    @Override
    public Q transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        // 组装对象
        AniHandler upsertHandler = new AniHandler();
        AniHandler.AniUpsertBatch aniUpsert = getBaseDmParserUtil().parserUpsertBatchSql(paramCondition, databaseConf, tableConf);
        upsertHandler.setAniUpsertBatch(aniUpsert);
        return (Q) upsertHandler;
    }
}
