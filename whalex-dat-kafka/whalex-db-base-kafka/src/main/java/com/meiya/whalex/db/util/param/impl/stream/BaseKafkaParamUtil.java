package com.meiya.whalex.db.util.param.impl.stream;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.stream.KafkaDatabaseInfo;
import com.meiya.whalex.db.entity.stream.KafkaHandler;
import com.meiya.whalex.db.entity.stream.KafkaTableInfo;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.Page;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.date.JodaTimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * kafka 参数转换工具
 */
@Slf4j
public class BaseKafkaParamUtil<Q extends KafkaHandler, D extends KafkaDatabaseInfo,
        T extends KafkaTableInfo> extends AbstractDbModuleParamUtil<Q, D, T> {
    @Override
    protected Q transitionListTableParam(QueryTablesCondition queryTablesCondition, D databaseConf) throws Exception {
        KafkaHandler kafkaHandler = new KafkaHandler();
        KafkaHandler.KafkaListTable kafkaListTable = new KafkaHandler.KafkaListTable();
        kafkaHandler.setKafkaListTable(kafkaListTable);
        kafkaListTable.setTableMatch(queryTablesCondition.getTableMatch());
        return (Q) kafkaHandler;
    }

    @Override
    protected Q transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, D databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected Q transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, D databaseConf, T tableConf) {
        KafkaHandler kafkaHandler = new KafkaHandler();
        KafkaHandler.KafkaResourceCreate kafkaResourceCreate = new KafkaHandler.KafkaResourceCreate();
        kafkaHandler.setKafkaResourceCreate(kafkaResourceCreate);
        kafkaResourceCreate.setNumPartitions(createTableParamCondition.getNumPartitions());
        kafkaResourceCreate.setReplicationFactor(createTableParamCondition.getReplicationFactor());
        kafkaResourceCreate.setRetentionTime(createTableParamCondition.getRetentionTime());
        return (Q) kafkaHandler;
    }

    @Override
    protected Q transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) new KafkaHandler();
    }

    @Override
    protected Q transitionCreateIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseKafkaParamUtil.transitionCreateIndexParam");
    }

    @Override
    protected Q transitionDropIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseKafkaParamUtil.transitionDropIndexParam");

    }

    @Override
    protected Q transitionQueryParam(QueryParamCondition queryParamCondition, D databaseConf, T tableConf) {
        KafkaHandler kafkaHandler = new KafkaHandler();
        KafkaHandler.KafkaResourceQuery kafkaResourceQuery = new KafkaHandler.KafkaResourceQuery();
        kafkaHandler.setKafkaResourceQuery(kafkaResourceQuery);
        Page page = queryParamCondition.getPage();
        kafkaResourceQuery.setCount(queryParamCondition.isCountFlag());
        if (page != null){
            Integer limit = page.getLimit();
            Integer offset = page.getOffset();
            kafkaResourceQuery.setLimit(limit.longValue());
            kafkaResourceQuery.setOffset(offset.longValue());
        }
        // 是否携带指定位移时间
        List<Where> where = queryParamCondition.getWhere();
        if (CollectionUtil.isNotEmpty(where)) {
            parserWhere(where, kafkaResourceQuery);
        }
        return (Q) kafkaHandler;
    }

    private void parserWhere(List<Where> wheres, KafkaHandler.KafkaResourceQuery kafkaResourceQuery) {
        for (int i = 0; i < wheres.size(); i++) {
            Where where = wheres.get(i);
            Rel type = where.getType();
            if (Rel.AND.equals(type) || Rel.OR.equals(type)) {
                parserWhere(where.getParams(), kafkaResourceQuery);
            } else {
                String field = where.getField();
                if (StringUtils.equalsIgnoreCase(field, "offsetTimestamp")) {
                    Rel whereType = where.getType();
                    Object param = where.getParam();
                    if (param != null) {
                        String offsetTimestampStr = param.toString();
                        if (Rel.GT.equals(whereType) || Rel.GTE.equals(whereType)) {
                            if (StringUtils.isNumeric(offsetTimestampStr)) {
                                kafkaResourceQuery.setBeginOffsetTimestamp(Long.parseLong(offsetTimestampStr));
                            } else {
                                try {
                                    Date time = DateUtils.parseDateStrictly(offsetTimestampStr, JodaTimeUtil.DEFAULT_YMD_FORMAT
                                            , JodaTimeUtil.DEFAULT_YMDHMS_FORMAT
                                            , JodaTimeUtil.SOLR_TDATE_FORMATE
                                            , JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE
                                            , JodaTimeUtil.COMPACT_YMDHMS_FORMAT
                                            , JodaTimeUtil.COMPACT_YMD_FORMAT
                                            , JodaTimeUtil.COMPACT_YMDH_FORMAT);
                                    kafkaResourceQuery.setBeginOffsetTimestamp(time.getTime());
                                } catch (ParseException e) {
                                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, e.getMessage());
                                }
                            }
                        } else if (Rel.LT.equals(whereType) || Rel.LTE.equals(whereType)) {
                            if (StringUtils.isNumeric(offsetTimestampStr)) {
                                kafkaResourceQuery.setEndOffsetTimestamp(Long.parseLong(offsetTimestampStr));
                            } else {
                                try {
                                    Date time = DateUtils.parseDateStrictly(offsetTimestampStr, JodaTimeUtil.DEFAULT_YMD_FORMAT
                                            , JodaTimeUtil.DEFAULT_YMDHMS_FORMAT
                                            , JodaTimeUtil.SOLR_TDATE_FORMATE
                                            , JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE
                                            , JodaTimeUtil.COMPACT_YMDHMS_FORMAT
                                            , JodaTimeUtil.COMPACT_YMD_FORMAT
                                            , JodaTimeUtil.COMPACT_YMDH_FORMAT);
                                    kafkaResourceQuery.setEndOffsetTimestamp(time.getTime());
                                } catch (ParseException e) {
                                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, e.getMessage());
                                }
                            }
                        }
                    }
                }
            }
        }
    }


    @Override
    protected Q transitionUpdateParam(UpdateParamCondition updateParamCondition, D databaseConf, T tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "KafkaParamUtil.transitionUpdateParam");
    }

    @Override
    protected Q transitionInsertParam(AddParamCondition addParamCondition, D databaseConf, T tableConf) {
        KafkaHandler kafkaHandler = new KafkaHandler();
        KafkaHandler.KafkaResourceInsert kafkaResourceInsert = new KafkaHandler.KafkaResourceInsert();
        kafkaHandler.setKafkaResourceInsert(kafkaResourceInsert);

        List<Map<String, Object>> fieldValueList = addParamCondition.getFieldValueList();
        List<KafkaHandler.KafkaMessage> kafkaInsertList = new ArrayList();
        for (int i = 0; i < fieldValueList.size(); i++) {
            Map<String, Object> jsonMap = fieldValueList.get(i);
            Object key = jsonMap.remove("__key");
            KafkaHandler.KafkaMessage kafkaMessage = new KafkaHandler.KafkaMessage();
            kafkaMessage.setKey(key);
            if(jsonMap.size() == 1 && jsonMap.get("__message") != null) {
                kafkaMessage.setMessage(jsonMap.get("__message"));
            }else {
                kafkaMessage.setMessage(JsonUtil.objectToStr(jsonMap));
            }
            kafkaInsertList.add(kafkaMessage);
        }
        kafkaResourceInsert.setKafkaInsertList(kafkaInsertList);
        return (Q) kafkaHandler;
    }

    @Override
    protected Q transitionDeleteParam(DelParamCondition delParamCondition, D databaseConf, T tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "KafkaParamUtil.transitionDeleteParam");
    }

    @Override
    protected Q transitionDropTableParam(DropTableParamCondition dropTableParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) new KafkaHandler();
    }

    @Override
    public Q transitionUpsertParam(UpsertParamCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "KafkaParamUtil.transitionUpsertParam");
    }

    @Override
    public Q transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + "transitionUpsertParamBatch");
    }
}
