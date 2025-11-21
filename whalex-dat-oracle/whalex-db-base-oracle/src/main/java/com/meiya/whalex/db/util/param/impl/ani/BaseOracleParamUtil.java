package com.meiya.whalex.db.util.param.impl.ani;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Filter;
import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BaseOracleDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseOracleTableInfo;
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
 * Oracle 参数转换工具类
 *
 * @author 蔡荣桂
 * @date 2021/4/14
 * @project whale-cloud-platformX
 */
//@DbParamUtil(dbType = DbResourceEnum.Oracle)
@Slf4j
public class BaseOracleParamUtil extends AbstractDbModuleParamUtil<AniHandler, BaseOracleDatabaseInfo, BaseOracleTableInfo> {
    BaseOracleParserUtil baseOracleParserUtil = new BaseOracleParserUtil();

    public BaseOracleParserUtil getBaseOracleParserUtil() {
        return baseOracleParserUtil;
    }

    @Override
    protected AniHandler transitionListTableParam(QueryTablesCondition queryTablesCondition, BaseOracleDatabaseInfo databaseInfo) throws Exception {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniListTable aniListTable = new AniHandler.AniListTable();
        aniHandler.setListTable(aniListTable);
        String tableMatch = queryTablesCondition.getTableMatch();
        tableMatch = StringUtils.replaceEach(tableMatch, new String[]{"*", "?"}, new String[]{"%", "_"});
        aniListTable.setTableMatch(tableMatch);
        return aniHandler;
    }

    @Override
    protected AniHandler transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, BaseOracleDatabaseInfo databaseInfo) throws Exception {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniListDatabase aniListDatabase = new AniHandler.AniListDatabase();
        aniHandler.setListDatabase(aniListDatabase);
        String databaseMatch = queryDatabasesCondition.getDatabaseMatch();
        databaseMatch = StringUtils.replaceEach(databaseMatch, new String[]{"*", "?"}, new String[]{"%", "_"});
        aniListDatabase.setDatabaseMatch(databaseMatch);
        return aniHandler;
    }

    @Override
    protected AniHandler transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniCreateTable(getBaseOracleParserUtil().parserCreateTableSql(createTableParamCondition, databaseInfo, tableInfo));

        return aniHandler;
    }

    @Override
    protected AniHandler transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) throws Exception {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniAlterTable(getBaseOracleParserUtil().parserAlterTableSql(alterTableParamCondition, databaseInfo, tableInfo));

        return aniHandler;

    }

    @Override
    protected AniHandler transitionCreateIndexParam(IndexParamCondition indexParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniCreateIndex aniCreateIndex = getBaseOracleParserUtil().parserCreateIndexSql(indexParamCondition, databaseInfo, tableInfo);
        aniHandler.setAniCreateIndex(aniCreateIndex);
        return aniHandler;
    }

    @Override
    protected AniHandler transitionDropIndexParam(IndexParamCondition indexParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniDropIndex aniDropIndex = getBaseOracleParserUtil().parserDropIndexSql(indexParamCondition, databaseInfo, tableInfo);
        aniHandler.setAniDropIndex(aniDropIndex);
        return aniHandler;
    }

    @Override
    protected AniHandler transitionQueryParam(QueryParamCondition queryParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniQuery aniQuery;
        // 如果存在自定义SQL，则不需要转换
        String sql = queryParamCondition.getSql();
        if (StringUtils.isNotBlank(sql)) {
            aniQuery = new AniHandler.AniQuery();
            aniQuery.setSql(sql);
        } else {
            aniQuery = getBaseOracleParserUtil().parserQuerySql(queryParamCondition, databaseInfo, tableInfo);
            aniQuery.setCount(queryParamCondition.isCountFlag());
            aniQuery.setBatchSize(queryParamCondition.getBatchSize());
        }
        aniHandler.setAniQuery(aniQuery);
        return aniHandler;
    }

    @Override
    protected AniHandler transitionUpdateParam(UpdateParamCondition updateParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniUpdate aniUpdate = getBaseOracleParserUtil().parserUpdateSql(updateParamCondition, databaseInfo, tableInfo);
        aniHandler.setAniUpdate(aniUpdate);
        return aniHandler;
    }

    @Override
    protected AniHandler transitionInsertParam(AddParamCondition addParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniInsert aniInsert = getBaseOracleParserUtil().parserInsertSql(addParamCondition, databaseInfo, tableInfo);
        aniHandler.setAniInsert(aniInsert);
        return aniHandler;
    }

    @Override
    protected AniHandler transitionDeleteParam(DelParamCondition delParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniDel(getBaseOracleParserUtil().parserDelSql(delParamCondition, databaseInfo, tableInfo));
        return aniHandler;
    }

    @Override
    protected AniHandler transitionDropTableParam(DropTableParamCondition dropTableParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) throws Exception {
        return new AniHandler();
    }

    @Override
    public AniHandler transitionUpsertParam(UpsertParamCondition paramCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) throws Exception {
        // Oracle 不使用自身 ON DUPLICATE KEY 方法，存在局限性，需要先设置 唯一索引
        // 1.获取指定 conflict 字段值构建查询条件
        Map<String, Object> updateParamMap = paramCondition.getUpsertParamMap();
        List<String> conflictFieldList = paramCondition.getConflictFieldList();
        // 拼装查询条件
        List<Where> collect = conflictFieldList.stream().flatMap(fieldName -> {
            Object value = updateParamMap.get(fieldName);
            String valueStr;
            if (value instanceof Date) {
                valueStr = DateUtil.format((Date) value, DatePattern.NORM_DATETIME_PATTERN);
            } else {
                valueStr = getBaseOracleParserUtil().formatter(String.valueOf(value));
            }
            return Stream.of(Where.create(fieldName, valueStr));
        }).collect(Collectors.toList());
        UpdateParamCondition updateParamCondition = new UpdateParamCondition();
        updateParamCondition.setWhere(collect);
        // 过滤掉指定 conflict 字段
        Map<String, Object> filter = MapUtil.filter(updateParamMap, (Filter<Map.Entry<String, Object>>) entry -> !conflictFieldList.contains(entry.getKey()));
        updateParamCondition.setUpdateParamMap(filter);
        // 转换更新 sql
        AniHandler aniHandler = transitionUpdateParam(updateParamCondition, databaseInfo, tableInfo);
        // 构建 upsert 对象
        AniHandler.AniUpsert aniUpsert = new AniHandler.AniUpsert();
        aniUpsert.setUpdateSql(aniHandler.getAniUpdate().getSql());
        aniUpsert.setUpdateParamArray(aniHandler.getAniUpdate().getParamArray());
        // 构建新增 sql
        AddParamCondition addParamCondition = new AddParamCondition();
        addParamCondition.setFieldValueList(Collections.singletonList(updateParamMap));
        AniHandler aniHandler1 = transitionInsertParam(addParamCondition, databaseInfo, tableInfo);
        aniUpsert.setInsertSql(aniHandler1.getAniInsert().getSql());
        // 组装对象
        AniHandler upsertHandler = new AniHandler();
        upsertHandler.setAniUpsert(aniUpsert);
        return upsertHandler;
    }

    @Override
    public AniHandler transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + "transitionUpsertParamBatch");
    }
}
