package com.meiya.whalex.db.util.param.impl.cache;

import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.cache.RedisDatabaseInfo;
import com.meiya.whalex.db.entity.cache.RedisHandler;
import com.meiya.whalex.db.entity.cache.RedisTableInfo;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.PublishMessage;
import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
import com.meiya.whalex.interior.db.operation.in.SubscribeMessage;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static com.meiya.whalex.exception.ExceptionCode.DB_PARAM_ERROR_EXCEPTION;

/**
 * redis 组件参数转换工具类
 *
 * 重要：
 * 目前只支持简单的key-value形式，后面想办法屏蔽客户端使用复杂性
 * 且redis组件支持更多复杂存储方式!!!
 *
 * @author chenjp
 * @date 2020/9/7
 */
//  TODO 目前只是处理简单key-value形式存储，后续想办法兼容更多(屏蔽客户端使用的复杂性) by chenjp
@Slf4j
//@DbParamUtil(dbType = DbResourceEnum.redis, version = DbVersionEnum.REDIS_4, cloudVendors = CloudVendorsEnum.MY)
public class BaseRedisParamUtil extends AbstractDbModuleParamUtil<RedisHandler, RedisDatabaseInfo, RedisTableInfo> {

    @Override
    protected RedisHandler transitionListTableParam(QueryTablesCondition queryTablesCondition, RedisDatabaseInfo databaseInfo) throws Exception {
        return new RedisHandler();
    }

    @Override
    protected RedisHandler transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, RedisDatabaseInfo databaseInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected RedisHandler transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, RedisDatabaseInfo databaseInfo, RedisTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisParamUtil.transitionCreateTableParam");
    }

    @Override
    protected RedisHandler transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, RedisDatabaseInfo databaseInfo, RedisTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisParamUtil.transitionAlterTableParam");
    }

    @Override
    protected RedisHandler transitionCreateIndexParam(IndexParamCondition indexParamCondition, RedisDatabaseInfo databaseInfo, RedisTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisParamUtil.transitionCreateIndexParam");
    }

    @Override
    protected RedisHandler transitionDropIndexParam(IndexParamCondition indexParamCondition, RedisDatabaseInfo databaseInfo, RedisTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisParamUtil.transitionDropIndexParam");
    }


    @Override
    protected RedisHandler transitionPublishMessage(PublishMessage paramCondition, RedisDatabaseInfo dataConf) {
        RedisHandler redisHandler = new RedisHandler();
        Object value = paramCondition.getValue();
        if(value == null) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "消息体不能为空");
        }
        RedisHandler.RedisPublishData redisPublishData = new RedisHandler.RedisPublishData();
        redisPublishData.setValue(value);
        redisHandler.setRedisPublishData(redisPublishData);
        return redisHandler;
    }

    @Override
    protected RedisHandler transitionSubscribeMessage(SubscribeMessage paramCondition, RedisDatabaseInfo dataConf) {
        RedisHandler redisHandler = new RedisHandler();
        Consumer<Object> consumer = paramCondition.getConsumer();
        if(consumer == null) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "消息处理对象不能为空");
        }
        RedisHandler.RedisSubscribeData redisSubscribeData = new RedisHandler.RedisSubscribeData();
        redisSubscribeData.setConsumer(consumer);
        redisSubscribeData.setAsync(paramCondition.isAsync());
        redisHandler.setRedisSubscribeData(redisSubscribeData);
        return redisHandler;
    }

    @Override
    protected RedisHandler transitionQueryParam(QueryParamCondition queryParamCondition, RedisDatabaseInfo databaseInfo, RedisTableInfo tableInfo) throws Exception {
        RedisHandler redisHandler = new RedisHandler();
        RedisHandler.RedisQuery redisQuery = new RedisHandler.RedisQuery();
        redisHandler.setRedisQuery(redisQuery);
        List<Where> wheres = queryParamCondition.getWhere();
        Where where = getWhere(wheres);
        redisQuery.setKey(where.getField());

        Rel type = where.getType();
        if(Rel.MGET.equals(type) || Rel.HMGET.equals(type)) {
            Object param = where.getParam();
            if(param instanceof List) {
                List paramList = (List) param;
                List<String> fieldList = new ArrayList<>();
                paramList.forEach(item->fieldList.add(item.toString()));
                redisQuery.setFields(fieldList);
            }else if(param instanceof Set) {
                Set paramSet = (Set) param;
                List<String> fieldList = new ArrayList<>();
                paramSet.forEach(item->fieldList.add(item.toString()));
                redisQuery.setFields(fieldList);
            }else {
                redisQuery.setFields(Arrays.asList(param.toString()));
            }
        }else {
            redisQuery.setField((String) where.getParam());
        }
        redisQuery.setRel(type);
        return redisHandler;
    }

    @Override
    protected RedisHandler transitionUpdateParam(UpdateParamCondition updateParamCondition, RedisDatabaseInfo databaseInfo, RedisTableInfo tableInfo) throws Exception {
        RedisHandler redisHandler = new RedisHandler();
        RedisHandler.RedisUpdate redisUpdate = new RedisHandler.RedisUpdate();
        redisHandler.setRedisUpdate(redisUpdate);
        List<Where> wheres = updateParamCondition.getWhere();
        Map<String, Object> updateParamMap = updateParamCondition.getUpdateParamMap();
        String key = getKey(wheres);
        redisUpdate.setKey(key);
        Object value = getValue(key, updateParamMap);
        if (value == null) {
            value = getValue(wheres);
        }
        redisUpdate.setValue(value);
        return redisHandler;
    }

    @Override
    protected RedisHandler transitionInsertParam(AddParamCondition addParamCondition, RedisDatabaseInfo databaseInfo, RedisTableInfo tableInfo) throws Exception {
        RedisHandler redisHandler = new RedisHandler();


        List<RedisHandler.RedisInsert> redisInsertList  = new ArrayList<>();

        redisHandler.setRedisInsertList(redisInsertList);
        List<Map<String, Object>> fieldValueList = addParamCondition.getFieldValueList();
        fieldValueList.forEach(fieldAndValue -> {

            Set<Map.Entry<String, Object>> entries = fieldAndValue.entrySet();
            Map.Entry<String, Object> next = entries.iterator().next();
            String key = next.getKey();
            Object fieldValue = next.getValue();


            RedisHandler.RedisInsert redisInsert = new RedisHandler.RedisInsert();

            redisInsert.setKey(key);

            if(fieldValue instanceof Map) {
                Map params = (Map) fieldValue;
                redisInsert.setField((String) params.get("field"));
                redisInsert.setValue(params.get("value"));
                redisInsert.setExpire((Long) params.get("expire"));
                String rel = (String) params.get("rel");
                if(rel != null) {
                    redisInsert.setRel(Rel.valueOf(rel));
                }
            }else if(fieldValue instanceof RedisHandler.RedisInsert){
                RedisHandler.RedisInsert params = (RedisHandler.RedisInsert) fieldValue;
                redisInsert.setField(params.getField());
                redisInsert.setValue(params.getValue());
                redisInsert.setExpire(params.getExpire());
                redisInsert.setRel(params.getRel());
            }else {
                redisInsert.setValue(fieldValue);
            }

            redisInsertList.add(redisInsert);

        });
        return redisHandler;
    }

    @Override
    protected RedisHandler transitionDeleteParam(DelParamCondition delParamCondition, RedisDatabaseInfo databaseInfo, RedisTableInfo tableInfo) throws Exception {
        RedisHandler redisHandler = new RedisHandler();
        RedisHandler.RedisDel redisDel = new RedisHandler.RedisDel();
        redisHandler.setRedisDel(redisDel);
        List<Where> wheres = delParamCondition.getWhere();
        Where where = getWhere(wheres);
        redisDel.setKey(where.getField());

        Rel type = where.getType();
        if(Rel.HDEL.equals(type)) {
            Object param = where.getParam();
            if(param instanceof List) {
                List paramList = (List) param;
                List<String> fieldList = new ArrayList<>();
                paramList.forEach(item->fieldList.add(item.toString()));
                redisDel.setFields(fieldList);
            }else {
                redisDel.setFields(Arrays.asList(param.toString()));
            }
        }else {
            redisDel.setField((String) where.getParam());
        }
        redisDel.setRel(type);
        return redisHandler;
    }

    @Override
    protected RedisHandler transitionDropTableParam(DropTableParamCondition dropTableParamCondition, RedisDatabaseInfo databaseInfo, RedisTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisParamUtil.transitionDropTableParam");
    }

    @Override
    public RedisHandler transitionUpsertParam(UpsertParamCondition paramCondition, RedisDatabaseInfo databaseInfo, RedisTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisParamUtil.transitionUpsertParam");
    }

    @Override
    public RedisHandler transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, RedisDatabaseInfo databaseInfo, RedisTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + "transitionUpsertParamBatch");
    }

    /**
     * 获取key
     */
    private String getKey(List<Where> wheres){
        judgeData(wheres);
        return wheres.get(0).getField();
    }

    /**
     * 获取where
     */
    private Where getWhere(List<Where> wheres){
        judgeData(wheres);
        return wheres.get(0);
    }

    /**
     * 获取value
     */
    private Object getValue(List<Where> wheres) {
        judgeData(wheres);
        return wheres.get(0).getParam();
    }

    private Object getValue(String key, Map<String, Object> updateParamMap) {
        if (MapUtil.isNotEmpty(updateParamMap)) {
            Object o = updateParamMap.get(key);
            return o;
        }
        return null;
    }

    /**
     * 判断数据是否存在
     */
    private void judgeData(List<Where> wheres){
        if(CollectionUtils.isEmpty(wheres) || wheres.size() < 1){
            throw new BusinessException(DB_PARAM_ERROR_EXCEPTION);
        }
    }
}
