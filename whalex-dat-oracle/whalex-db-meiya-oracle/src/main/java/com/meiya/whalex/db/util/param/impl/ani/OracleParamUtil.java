package com.meiya.whalex.db.util.param.impl.ani;


import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BaseOracleDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseOracleTableInfo;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.meiya.whalex.interior.db.search.out.FieldEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Oracle 参数转换工具类
 *
 * @author 蔡荣桂
 * @date 2021/4/14
 * @project whale-cloud-platformX
 */
@DbParamUtil(dbType = DbResourceEnum.oracle, version = DbVersionEnum.ORACLE_11G, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class OracleParamUtil extends BaseOracleParamUtil {

    @Override
    protected AniHandler transitionUpdateParam(UpdateParamCondition updateParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {
        List<FieldEntity> fieldEntityList = updateParamCondition.getFieldEntityList();
        if(fieldEntityList != null) {
            Map<String, Object> objectMap = parserFieldEntity(fieldEntityList);
            updateParamCondition.setUpdateParamMap(objectMap);
        }
        return super.transitionUpdateParam(updateParamCondition, databaseInfo, tableInfo);
    }

    @Override
    protected AniHandler transitionInsertParam(AddParamCondition addParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {
        List<List<FieldEntity>> fieldEntityList = addParamCondition.getFieldEntityList();
        if (fieldEntityList != null) {
            List<Map<String, Object>> collect = fieldEntityList.stream().map(item -> {
                return parserFieldEntity(item);
            }).collect(Collectors.toList());
            addParamCondition.setFieldValueList(collect);
        }
        return super.transitionInsertParam(addParamCondition, databaseInfo, tableInfo);
    }

    /**
     * 由于com.meiya.whalex.handler.FieldParserHandler#parserFieldEntity加入date字段的解析，可能会对现有的组件有影响，故本组件先自己解析
     *  com.meiya.whalex.handler.FieldParserHandler#parserFieldEntity加入date字段的解析，方法加入对date字段的解析后，可以删除
     * @param fieldEntities
     * @return
     */
    private Map<String, Object> parserFieldEntity(List<FieldEntity> fieldEntities) {
        if (CollectionUtil.isEmpty(fieldEntities)) {
            return null;
        }
        Map<String, Object> parserMap = new LinkedHashMap<>();
        for (int t = 0; t < fieldEntities.size(); t++) {
            FieldEntity fieldEntity = fieldEntities.get(t);
            String field = fieldEntity.getField();
            Object value = fieldEntity.getValue();
            String fieldType = fieldEntity.getFieldType();
            if (StringUtils.equalsIgnoreCase(fieldType, "Base64String")) {
                if (value instanceof Map) {
                    Map<String, byte[]> convertMap = new LinkedHashMap<>();
                    Map<String, Object> paramMap = (Map<String, Object>) value;
                    for (Map.Entry<String, Object> entry : paramMap.entrySet()) {
                        String key = entry.getKey();
                        String mapValue = (String) entry.getValue();
                        byte[] bytes = Base64.decodeBase64(mapValue);
                        convertMap.put(key, bytes);
                    }
                    parserMap.put(field, convertMap);
                } else {
                    parserMap.put(field, Base64.decodeBase64((String) value));
                }
            } else if (StringUtils.equalsIgnoreCase(fieldType, ItemFieldTypeEnum.DATE.getVal())){
                parserMap.put(field, DateUtil.parse((String)value, DatePattern.NORM_DATETIME_PATTERN));
            } else {
                parserMap.put(field, value);
            }
        }
        return parserMap;
    }
}
