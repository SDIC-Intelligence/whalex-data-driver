package com.meiya.whalex.db.util.param.impl.lucene;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapBuilder;
import cn.hutool.core.map.MapUtil;
import com.google.common.collect.Lists;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.lucene.EsDatabaseInfo;
import com.meiya.whalex.db.entity.lucene.EsFieldTypeEnum;
import com.meiya.whalex.db.entity.lucene.EsHandler;
import com.meiya.whalex.db.entity.lucene.EsTableInfo;
import com.meiya.whalex.db.util.common.JsonMap;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CommonConstant;
import com.meiya.whalex.interior.db.operation.in.*;
import com.meiya.whalex.interior.db.search.condition.*;
import com.meiya.whalex.interior.db.search.in.*;
import com.meiya.whalex.util.JsonIncludeAlwaysUtil;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.date.DateUtil;
import com.meiya.whalex.util.date.JodaTimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;


import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.meiya.whalex.interior.db.search.condition.Rel.GEO_DISTANCE;

/**
 * Es 参数转换工具
 *
 * @author 黄河森
 * @date 2019/12/19
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseEsParamUtil<Q extends EsHandler, D extends EsDatabaseInfo,
        T extends EsTableInfo> extends AbstractDbModuleParamUtil<Q, D, T> {
    private static final String schema = "{\"settings\":{\"index\":{\"codec\":\"best_compression\",\"refresh_interval\":\"120s\",\"number_of_shards\":2,\"translog\":{\"retention.size\":\"512m\",\"durability\":\"async\",\"flush_threshold_size\":\"5GB\"},\"store.type\":\"niofs\",\"number_of_replicas\":1,\"routing.allocation.total_shards_per_node\":\"2\"},\"analysis\":{\"char_filter\":{\"dot_patter\":{\"pattern\":\"\\\\.\",\"type\":\"pattern_replace\",\"replacement\":\" \"}},\"analyzer\":{\"my_analyzer\":{\"filter\":[\"lowercase\"],\"char_filter\":[\"dot_patter\"],\"tokenizer\":\"standard\"},\"rev_analyzer\":{\"filter\":[\"lowercase\",\"reverse\"],\"tokenizer\":\"keyword\"},\"lowercase_analyzer\":{\"filter\":[\"lowercase\"],\"tokenizer\":\"keyword\"}},\"tokenizer\":{\"dot_tokenizer\":{\"pattern\":\"\\\\.\",\"type\":\"simple_pattern_split\"}}}},\"mappings\":{\"_source\":{\"excludes\":[\"content\",\"content_en\",\"content_cn\"]},\"dynamic_templates\":[{\"match_loc\":{\"mapping\":{\"type\":\"geo_point\"},\"match_mapping_type\":\"*\",\"match\":\"*_loc\"}},{\"match_coordinate\":{\"mapping\":{\"type\":\"double\"},\"match_mapping_type\":\"*\",\"match\":\"*_coordinate\"}},{\"match_s\":{\"mapping\":{\"type\":\"keyword\"},\"match_mapping_type\":\"*\",\"match\":\"*_s\"}},{\"match_b\":{\"mapping\":{\"type\":\"boolean\"},\"match_mapping_type\":\"*\",\"match\":\"*_b\"}},{\"match_i\":{\"mapping\":{\"type\":\"integer\"},\"match_mapping_type\":\"*\",\"match\":\"*_i\"}},{\"match_f\":{\"mapping\":{\"type\":\"float\"},\"match_mapping_type\":\"*\",\"match\":\"*_f\"}},{\"match_l\":{\"mapping\":{\"type\":\"long\"},\"match_mapping_type\":\"*\",\"match\":\"*_l\"}},{\"match_d\":{\"mapping\":{\"type\":\"double\"},\"match_mapping_type\":\"*\",\"match\":\"*_d\"}},{\"match_dt\":{\"mapping\":{\"type\":\"date\"},\"match_mapping_type\":\"*\",\"match\":\"*_dt\"}},{\"match_t\":{\"mapping\":{\"analyzer\":\"my_analyzer\",\"type\":\"text\"},\"match_mapping_type\":\"*\",\"match\":\"*_t\"}},{\"match_c\":{\"mapping\":{\"type\":\"keyword\"},\"match_mapping_type\":\"*\",\"match\":\"*C\"}},{\"default2keywrod\":{\"mapping\":{\"type\":\"keyword\"},\"match_mapping_type\":\"string\",\"match\":\"*\"}}],\"properties\":{\"a\":{\"type\":\"string\"},\"b\":{\"type\":\"string\"},\"c\":{\"type\":\"string\"}}},\"index_patterns\":[\"test_zhulixin_1013a_*\"],\"order\":0}";

    private static final String UPSERT_TEMPLATE = "{\"update\":{\"_index\":\"${indexName}\",\"_type\":\"${docType}\",\"_id\":\"${_id}\"}}\n" +
            "{\"doc\":${doc},\"doc_as_upsert\":true}\n";

    @Override
    protected Q transitionListTableParam(QueryTablesCondition queryTablesCondition, D databaseConf) throws Exception {
        EsHandler esHandler = new EsHandler();
        EsHandler.EsListTable esListTable = new EsHandler.EsListTable();
        esHandler.setEsListTable(esListTable);
        esListTable.setIndexMatch(queryTablesCondition.getTableMatch());
        return (Q) esHandler;
    }

    @Override
    protected Q transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, D databaseConf) throws Exception {
        return (Q) new EsHandler();
    }

    @Override
    protected Q transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, D databaseConf, T tableConf) {
        EsHandler esHandler = new EsHandler();
        EsHandler.EsCreateTable createTable = new EsHandler.EsCreateTable();
        esHandler.setCreateTable(createTable);

        //将建索引list条件转化为map
        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();
        if (CollectionUtils.isNotEmpty(createTableFieldParamList)) {
            // 设置 es doc的properties
            Map<String, Object> schemaMap = JsonUtil.jsonStrToMap(schema);
            Map<String, Map<String, Object>> properties = new HashMap<>();

            Map<String, Object> mappings = (Map<String, Object>) schemaMap.get("mappings");
            Map<String, Object> settings = (Map<String, Object>) schemaMap.get("settings");
            //增加分词器定义
            Map<String, Object> analyzerDefine = createTableParamCondition.getAnalyzerDefine();
            Map<String, Object> analysisDefine = createTableParamCondition.getAnalysis();
            if(CollectionUtil.isNotEmpty(analysisDefine)) {
                Map<String, Object> analysis = (Map<String, Object>) settings.get("analysis");
                Map<String, Object> analyzer = (Map<String, Object>) analysis.get("analyzer");
                Map<String, Object> char_filter = (Map<String, Object>) analysis.get("char_filter");
                Map<String, Object> tokenizer = (Map<String, Object>) analysis.get("tokenizer");

                Map<String, Object> analyzerMap = (Map<String, Object>) analysisDefine.get("analyzer");
                Map<String, Object> char_filterMap = (Map<String, Object>) analysisDefine.get("char_filter");
                Map<String, Object> tokenizerMap = (Map<String, Object>) analysisDefine.get("tokenizer");
                if(CollectionUtil.isNotEmpty(analyzerMap)) {
                    analyzer.putAll(analyzerMap);
                }
                if(CollectionUtil.isNotEmpty(char_filterMap)) {
                    char_filter.putAll(char_filterMap);
                }
                if(CollectionUtil.isNotEmpty(tokenizerMap)) {
                    tokenizer.putAll(tokenizerMap);
                }


            }else if(CollectionUtil.isNotEmpty(analyzerDefine)) {
                Map<String, Object> analysis = (Map<String, Object>) settings.get("analysis");
                Map<String, Object> analyzer = (Map<String, Object>) analysis.get("analyzer");
                analyzer.putAll(analyzerDefine);
            }

            if(createTableParamCondition.isDefaultConf()) {
                //默认设置三个字段，content，content_en,content_cn
                Map<String, Object> contentMap = new HashMap<>();
                contentMap.put("type", EsFieldTypeEnum.TEXT.getDbFieldType());
                contentMap.put("analyzer", "my_analyzer");
                properties.put("content", contentMap);
                properties.put("content_cn", contentMap);
                properties.put("content_en", contentMap);
            }else {
                mappings.remove("_source");
                mappings.remove("dynamic_templates");
            }

            //设置新增的字段
            setTableField(createTableFieldParamList,properties);

            // 设置最大窗口
            Long maxResultWindow = tableConf.getMaxResultWindow();
            if (maxResultWindow != null) {
                Map<String, Object> index = (Map<String, Object>) settings.get("index");
                index.put("max_result_window", maxResultWindow);
            }

            mappings.put("properties", properties);
            createTable.setSchemaTemPlate(schemaMap);
        }

        String startTime = createTableParamCondition.getStartTime();
        String endTime = createTableParamCondition.getEndTime();
        if (StringUtils.isNotBlank(startTime) && StringUtils.isNotBlank(endTime)) {
            Date startDate = DateUtil.convertToDate(createTableParamCondition.getStartTime(), JodaTimeUtil.DEFAULT_YMD_FORMAT);
            Date endDate = DateUtil.convertToDate(createTableParamCondition.getEndTime(), JodaTimeUtil.DEFAULT_YMD_FORMAT);
            createTable.setStartTime(startDate);
            createTable.setStopTime(endDate);
        }
        return (Q) esHandler;
    }

    protected void setTableField(List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList, Map properties){
        if (CollectionUtil.isNotEmpty(createTableFieldParamList)){
            for (CreateTableParamCondition.CreateTableFieldParam element : createTableFieldParamList) {
                Map<String, Object> map = new HashMap<>();
                String fieldName = element.getFieldName();
                if (StringUtils.isBlank(fieldName)) {
                    throw new BusinessException("创建表操作执行失败, 字段名称为null");
                }

                boolean analyzer = element.isAnalyzer();

                String dbFiledType;

                String fieldType = element.getFieldType();

                EsFieldTypeEnum fieldTypeEnum = EsFieldTypeEnum.findFieldTypeEnum(fieldType);

                switch (fieldTypeEnum) {
                    case JSON:
                        dbFiledType = EsFieldTypeEnum.JSON.getDbFieldType();
                        Map<String, Map<String, Object>> nestedMapping = new HashMap();
                        setTableField(element.getObjectFieldParamList(),nestedMapping);
                        map.put("properties",nestedMapping);
                        break;
                    case TEXT:
                    case TINYTEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                    case ARRAY:
                    case ARRAY_TEXT:
                    case ARRAY_TINYTEXT:
                    case ARRAY_MEDIUMTEXT:
                    case ARRAY_LONGTEXT:
                        dbFiledType = fieldTypeEnum.getDbFieldType();
                        analyzer = true;
                        break;
                    case DATE:
                    case DATETIME:
                    case DATETIME_2:
                    case TIMESTAMP:
                        dbFiledType = fieldTypeEnum.getDbFieldType();
                        //如果是data类型，设置默认的format
                        map.put("format", "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis||dateOptionalTime");
                        break;
                    case YEAR:
                        dbFiledType = fieldTypeEnum.getDbFieldType();
                        //如果是data类型，设置默认的format
                        map.put("format", "yyyy");
                        break;
                    case TIME:
                        dbFiledType = fieldTypeEnum.getDbFieldType();
                        //如果是data类型，设置默认的format
                        map.put("format", "HH:mm:ss");
                        break;
                    default:
                        dbFiledType = fieldTypeEnum.getDbFieldType();
                        break;
                }

                if (analyzer && StringUtils.equalsIgnoreCase(dbFiledType, "text")) {
                    // 是否设置指定分词器
                    if (StringUtils.isNotBlank(element.getTokenizer())) {
                        map.put("analyzer", element.getTokenizer());
                    }
                    if (element.isExtendKeyword()) {
                        Map<String, Object> keywordMap = new HashMap<>(1);
                        map.put("fields", keywordMap);
                        Map<String, Object> keywordConfMap = new HashMap<>(2);
                        keywordConfMap.put("type", "keyword");
                        keywordConfMap.put("ignore_above", element.getKeywordIgnoreAbove());
                        keywordMap.put("keyword", keywordConfMap);
                    }
                } else if (StringUtils.equalsIgnoreCase(dbFiledType, "text") && !analyzer) {
                    dbFiledType = "keyword";
                }

                map.put("type", StringUtils.lowerCase(dbFiledType));

                //设置copy_to
                if (StringUtils.isNotBlank(element.getCopyToField())) {
                    map.put("copy_to", element.getCopyToField());
                }

                properties.put(fieldName, map);
            }
        }
    }

    private List<CreateTableParamCondition.CreateTableFieldParam> translate(List<AlterTableParamCondition.AddTableFieldParam> addTableFieldParams) {

        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParams = new ArrayList<>(addTableFieldParams.size());

        for (AlterTableParamCondition.AddTableFieldParam addTableFieldParam : addTableFieldParams) {

            CreateTableParamCondition.CreateTableFieldParam createTableFieldParam = new CreateTableParamCondition.CreateTableFieldParam();
            createTableFieldParam.setFieldName(addTableFieldParam.getFieldName());
            createTableFieldParam.setFieldComment(addTableFieldParam.getFieldComment());
            createTableFieldParam.setFieldType(addTableFieldParam.getFieldType());
            createTableFieldParam.setFieldLength(addTableFieldParam.getFieldLength());
            createTableFieldParam.setFieldDecimalPoint(addTableFieldParam.getFieldDecimalPoint());
            createTableFieldParam.setDistributed(addTableFieldParam.isDistributed());
            createTableFieldParam.setPrimaryKey(addTableFieldParam.isPrimaryKey());
            createTableFieldParam.setNotNull(addTableFieldParam.getNotNull() == null ? false : addTableFieldParam.getNotNull());
            createTableFieldParam.setAnalyzer(addTableFieldParam.isAnalyzer());
            createTableFieldParam.setTokenizer(addTableFieldParam.getTokenizer());
            createTableFieldParam.setCopyToField(addTableFieldParam.getCopyToField());
            createTableFieldParam.setExtendKeyword(addTableFieldParam.isExtendKeyword());
            if(addTableFieldParam.getObjectFieldParamList() != null) {
                createTableFieldParam.setObjectFieldParamList(translate(addTableFieldParam.getObjectFieldParamList()));
            }
            createTableFieldParam.setDefaultValue(addTableFieldParam.getDefaultValue());
            createTableFieldParams.add(createTableFieldParam);

        }

        return createTableFieldParams;
    }

    @Override
    protected Q transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, D databaseConf, T tableConf) throws Exception {
        EsHandler esHandler = new EsHandler();
        EsHandler.EsAlterField esAlterField = new EsHandler.EsAlterField();
        esHandler.setEsAlterField(esAlterField);
        Map<String, Object> addFieldMap = new HashMap<>();
        List<String> deleteFieldList = alterTableParamCondition.getDelTableFieldParamList();
        esAlterField.setAddFieldMap(addFieldMap);
        esAlterField.setDeleteFieldList(deleteFieldList);

        List<AlterTableParamCondition.AddTableFieldParam> addTableFieldParamList = alterTableParamCondition.getAddTableFieldParamList();
        if (CollectionUtils.isNotEmpty(addTableFieldParamList)) {

            List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParams = translate(addTableFieldParamList);

            setTableField(createTableFieldParams, addFieldMap);
        }

        //别名
        esAlterField.setAlias(alterTableParamCondition.getNewTableName());


        return (Q) esHandler;
    }

    @Override
    protected Q transitionCreateIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) {
        return (Q) new EsHandler();
    }

    @Override
    protected Q transitionDropIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) new EsHandler();
    }

    @Override
    protected Q transitionQueryParam(QueryParamCondition queryParamCondition, D databaseConf, T tableConf) {
        EsHandler esHandler = new EsHandler();
        EsHandler.EsQuery esQuery = new EsHandler.EsQuery();
        esHandler.setEsQuery(esQuery);

        // 如果配置了路由字段，且字段再where中包含，则配置路由值
        if (StringUtils.isNotBlank(queryParamCondition.getRoutingField())) {
            Object value = isIncludeRoutingField(queryParamCondition.getWhere(), queryParamCondition.getRoutingField());
            if (value != null && isPrimitive(value)) {
                esQuery.setRoutingValue(value);
            }
        }

        // 设置可查询字段
        List<String> fields = queryParamCondition.getSelect();
        List<String> hiddenFields = new ArrayList<>();
        if (fields != null && fields.size() > 0) {
            fields = new ArrayList<>(fields);
            boolean flag = false;
            Iterator<String> iterator = fields.iterator();
            while (iterator.hasNext()) {
                String currentField = iterator.next();
                if (("_id".equals(currentField))) {
                    iterator.remove();
                    flag = true;
                }
                // 隐藏字段
                if (StringUtils.startsWithIgnoreCase(currentField, "~")) {
                    hiddenFields.add(StringUtils.removeStartIgnoreCase(currentField, "~"));
                    iterator.remove();
                }
            }
            if (fields.size() > 0 || flag) {
                // 若未显示指定 查询结果包含id，则需要默认带上
                if (!fields.contains("id")) {
                    fields.add("id");
                }
                esQuery.setFields(fields);
            }
            esQuery.setHiddenFields(hiddenFields);
        }

        // 设置查询条件
        List<Where> where = queryParamCondition.getWhere();
        // 查询条件
        Map<String, String> paramMap = new HashMap<>();
        // 查询条件解析
        if (CollectionUtils.isNotEmpty(where)) {
            BurstZone burstZone = new BurstZone();
            parserWhere(where, paramMap, burstZone);
            esQuery.setStartTime(burstZone.getStartTime());
            esQuery.setStopTime(burstZone.getStopTime());
        }

        // 高亮条件解析
        Map<String, String> highlightMap = new HashMap<>();
        List<String> highlightFields = queryParamCondition.getHighlightFields();
        if(CollectionUtils.isNotEmpty(highlightFields)) {
            parserHighlight(highlightFields,
                    queryParamCondition.getHighlightNum(),
                    queryParamCondition.getPreTags(),
                    queryParamCondition.getPostTags(),
                    highlightMap);
        }

        // 设置是否需要统计数据量
        esQuery.setCount(queryParamCondition.isCountFlag());

        // 是否对查询分表时间范围进行限制
        if (queryParamCondition.isLimitDate()) {
            String startTime = esQuery.getStartTime();
            String stopTime = esQuery.getStopTime();

            if (StringUtils.isBlank(startTime) || StringUtils.isBlank(stopTime)) {
                Date date = DateUtil.addDay(DateUtil.now(), -7);
                String startDate = DateUtil.formatDate(date, JodaTimeUtil.DEFAULT_YMDHMS_FORMAT);
                esQuery.setStopTime(DateUtil.formatDate(DateUtil.now(), JodaTimeUtil.DEFAULT_YMDHMS_FORMAT));
                esQuery.setStartTime(startDate);
            }
        }

        //分页
        Page page = queryParamCondition.getPage();
        if (null != page) {
            esQuery.setFrom(page.getOffset());
            esQuery.setSize(page.getLimit());
        }

        // 聚合函数
        Map<String, Object> aggMap = null;
        if (CollectionUtil.isNotEmpty(queryParamCondition.getAggList())) {
            List<Aggs> aggList = queryParamCondition.getAggList();
            aggMap = aggList.stream().flatMap(agg -> Stream.of(aggs2esAggMap(agg)))
                    .reduce((a, b) -> {
                        a.putAll(b);
                        return a;
                    }).orElse(null);
            esQuery.setAgg(true);
        } else {
            Aggs aggs = queryParamCondition.getAggs();
            if (aggs != null) {
                //aggs 的对象，不包含aggs
                esQuery.setAgg(true);
                aggMap = aggs2esAggMap(aggs);
            }else {
                List<AggFunction> aggFunctionList = queryParamCondition.getAggFunctionList();
                if(CollectionUtil.isNotEmpty(aggFunctionList)) {
                    aggMap = aggFunctionToEsAggMap(aggFunctionList);
                    esQuery.setAgg(true);
                }
            }
        }

        // 排序条件
        Map<String, Sort> sortMap = new LinkedHashMap<>();
        List<Order> orders = queryParamCondition.getOrder();
        if (orders != null && orders.size() != 0) {
            for (Order order : orders) {
                sortMap.put(order.getField(), order.getSort());
            }

        }

        //推荐查询
        Suggest suggest = queryParamCondition.getSuggest();
        Map<String, Object> suggestMap = null;
        // 术语推荐解析
        if (suggest != null) {
            // term 解析
            List<TermSuggest> termSuggests = suggest.getTermSuggests();
            suggestMap = new HashMap<>();
            if (CollectionUtil.isNotEmpty(termSuggests)) {
                Map<String, Object> termSuggestMap = parserTermSuggest(termSuggests);
                suggestMap.putAll(termSuggestMap);
                esQuery.setFrom(0);
                esQuery.setSize(0);
            }
            // 短语解析
            List<PhraseSuggest> phraseSuggests = suggest.getPhraseSuggests();
            if (CollectionUtil.isNotEmpty(phraseSuggests)) {
                Map<String, Object> phraseSuggestMap = parserPhraseSuggest(phraseSuggests);
                suggestMap.putAll(phraseSuggestMap);
                esQuery.setFrom(0);
                esQuery.setSize(0);
            }
            // 自动补全
            List<CompletionSuggest> completionSuggests = suggest.getCompletionSuggests();
            if (CollectionUtil.isNotEmpty(completionSuggests)) {
                Map<String, Object> completionSuggestMap = parserCompletionSuggest(completionSuggests);
                suggestMap.putAll(completionSuggestMap);
                esQuery.setFrom(0);
                esQuery.setSize(0);
            }
        }
        // 如果有传查询DSL，优先使用DSL查询
        if (queryParamCondition.getDsl() != null) {
            Object dslObj = queryParamCondition.getDsl();
            String dsl = JsonUtil.objectToStr(dslObj);
            if (StringUtils.contains(dsl, "aggs") || StringUtils.contains(dsl, "aggregations")) {
                esQuery.setAgg(true);
            }
            Map<String, Object> map = JsonUtil.jsonStrToMap(dsl);
            if (map.get("from") != null && map.get("size") != null) {
                esQuery.setFrom((Integer) map.get("from"));
                esQuery.setSize((Integer) map.get("size"));
                map.remove("from");
                map.remove("size");
            }
            if (map.containsKey("timeout")) {
                Object timeOut = map.get("timeout");
                if (timeOut != null) {
                    String tout = String.valueOf(timeOut);
                    if (StringUtils.isNotBlank(tout)) {
                        tout = StringUtils.replace(tout, "ms", "");
                        if (StringUtils.isNumeric(tout)) {
                            esQuery.setTimeOut(Integer.valueOf(tout));
                        } else {
                            log.warn("自定义 DSL 中 timeOut {} 值格式不正确!", timeOut);
                        }
                    }
                }
            }
            esQuery.setQueryJson(JsonUtil.objectToStr(map));
        } else {
            // 拼接查询语句
            String requestJson = transitionEsRequestJson(fields, paramMap, highlightMap, sortMap, aggMap, esQuery.isCount(), suggestMap);
            esQuery.setQueryJson(requestJson);
//            esQuery.setQueryHttpEntity(new NStringEntity(requestJson, ContentType.APPLICATION_JSON));
        }

        // 是否指定主分片查询
        if (queryParamCondition.isPrimaryNode()){
            esQuery.setPrimaryNode(true);
        }
        esQuery.setBatchSize(queryParamCondition.getBatchSize());
        esQuery.setPreference(queryParamCondition.getPreference());
        esQuery.setTerminateAfter(queryParamCondition.getTerminateAfter());
        return (Q) esHandler;
    }

    /**
     * 高亮解析
     * @param highlightFields 高亮字段
     * @param highlightNum 高亮数
     * @param preTags 前缀
     * @param postTags 后缀
     * @param highlightMap
     */
    private void parserHighlight(List<String> highlightFields, int highlightNum, String preTags, String postTags, Map<String, String> highlightMap) {
        //"highlight":{"fields":{"graph_id.keyword":{},"graph_id":{}},"pre_tags":"<b>","post_tags":"</b>","number_of_fragments":5}}
        StringBuilder fieldsBuilder = new StringBuilder();
        for (int i = 0; i < highlightFields.size(); i++) {
            if(i != 0) {
                fieldsBuilder.append(",");
            }
            fieldsBuilder.append("\"").append(highlightFields.get(i)).append("\"").append(": {}");
        }

        StringBuilder highlightBuilder = new StringBuilder();
        highlightBuilder.append("{")
                .append("\"fields\":{").append(fieldsBuilder.toString()).append("}");

        if(StringUtils.isNotBlank(preTags) && StringUtils.isNotBlank(postTags)) {
            highlightBuilder.append(",\"pre_tags\":\"")
                    .append(preTags).append("\",\"post_tags\":\"")
                    .append(postTags).append("\"");

        }

        if(highlightNum != 5) {
            highlightBuilder.append(",\"number_of_fragments\":").append(highlightNum);

        }

        highlightBuilder.append("}");

        highlightMap.put("highlight", highlightBuilder.toString());
    }

    /**
     * 解析自动补全推荐语法
     *
     * @param completionSuggests
     * @return
     */
    private Map<String, Object> parserCompletionSuggest(List<CompletionSuggest> completionSuggests) {
        Map<String, Object> phraseMap = new HashMap<>(1);
        for (int i = 0; i < completionSuggests.size(); i++) {
            CompletionSuggest completionSuggest = completionSuggests.get(i);
            Map<String, Object> bodyParamMap = new HashMap<>(2);
            phraseMap.put(completionSuggest.getSuggestName(), bodyParamMap);
            if (StringUtils.isNotBlank(completionSuggest.getPrefix())) {
                bodyParamMap.put("prefix", completionSuggest.getPrefix());
            } else if (StringUtils.isNotBlank(completionSuggest.getRegex())) {
                bodyParamMap.put("regex", completionSuggest.getRegex());
            } else {
                bodyParamMap.put("text", completionSuggest.getText());
            }
            Map<String, Object> completionParamMap = new HashMap<>();
            bodyParamMap.put("completion", completionParamMap);
            completionParamMap.put("field", completionSuggest.getField());
            if (completionSuggest.getSkipDuplicate() != null && completionSuggest.getSkipDuplicate()) {
                completionParamMap.put("skip_duplicates", completionSuggest.getSkipDuplicate());
            }
            if (completionSuggest.getSize() != null) {
                completionParamMap.put("size", completionSuggest.getSize());
            }
            if (completionSuggest.getFuzzyQuery() != null) {
                CompletionSuggest.Fuzzy fuzzyQuery = completionSuggest.getFuzzyQuery();
                Map<String, Object> fuzzyMap = new HashMap<>(1);
                completionParamMap.put("fuzzy", fuzzyMap);
                if (fuzzyQuery.getFuzziness() != null) {
                    fuzzyMap.put("fuzziness", fuzzyQuery.getFuzziness());
                }
                if (fuzzyQuery.getTranspositions() != null) {
                    fuzzyMap.put("transpositions", fuzzyQuery.getTranspositions());
                }
                if (fuzzyQuery.getUnicodeAware() != null) {
                    fuzzyMap.put("unicode_aware", fuzzyQuery.getUnicodeAware());
                }
                if (fuzzyQuery.getMinLength() != null) {
                    fuzzyMap.put("min_length", fuzzyQuery.getMinLength());
                }
                if (fuzzyQuery.getPrefixLength() != null) {
                    fuzzyMap.put("prefix_length", fuzzyQuery.getPrefixLength());
                }
            }
        }
        return phraseMap;
    }

    /**
     * 短语匹配
     *
     * @param phraseSuggests
     */
    private Map<String, Object> parserPhraseSuggest(List<PhraseSuggest> phraseSuggests) {
        Map<String, Object> phraseMap = new HashMap<>();
        for (int i = 0; i < phraseSuggests.size(); i++) {
            PhraseSuggest phraseSuggest = phraseSuggests.get(i);
            Map<String, Object> bodyParamMap = new HashMap<>(2);
            phraseMap.put(phraseSuggest.getSuggestName(), bodyParamMap);
            bodyParamMap.put("text", phraseSuggest.getText());
            Map<String, Object> phraseParamMap = new HashMap<>();
            bodyParamMap.put("phrase", phraseParamMap);
            phraseParamMap.put("field", phraseSuggest.getField());
            if (phraseSuggest.getGramSize() != null) {
                phraseParamMap.put("gram_size", phraseSuggest.getGramSize());
            }
            if (phraseSuggest.getDirectGenerators() != null) {
                List<PhraseSuggest.DirectGenerator> directGenerators = phraseSuggest.getDirectGenerators();
                List<Map<String, String>> directs = directGenerators.stream().flatMap(directGenerator -> Stream.of(MapUtil.builder("field", directGenerator.getField()).put("suggest_mode", directGenerator.getMode().getName()).build())).collect(Collectors.toList());
                phraseParamMap.put("direct_generator", directs);
            }
        }
        return phraseMap;
    }

    /**
     * 术语推荐查询语法解析
     *
     * @param termSuggests
     * @return
     */
    private Map<String, Object> parserTermSuggest(List<TermSuggest> termSuggests) {
        Map<String, Object> termSuggestMap = new HashMap<>(termSuggests.size());
        for (int i = 0; i < termSuggests.size(); i++) {
            Map<String, Object> bodyParamMap = new HashMap<>(2);
            TermSuggest termSuggest = termSuggests.get(i);
            String suggestName = termSuggest.getSuggestName();
            termSuggestMap.put(suggestName, bodyParamMap);
            bodyParamMap.put("text", termSuggest.getText());
            Map<String, Object> termParamMap = new HashMap<>(4);
            bodyParamMap.put("term", termParamMap);
            termParamMap.put("field", termSuggest.getField());
            if (termSuggest.getSize() != null) {
                termParamMap.put("size", termSuggest.getSize());
            }
            if (termSuggest.getSort() != null) {
                termParamMap.put("sort", termSuggest.getSort().getName());
            }
            if (termSuggest.getMode() != null) {
                termParamMap.put("suggest_mode", termSuggest.getMode().getName());
            }
        }
        return termSuggestMap;
    }

    /**
     * 递归获取where中的路由值
     *
     * @param where
     * @param routingField
     * @return
     */
    private Object isIncludeRoutingField(List<Where> where, String routingField) {
        if (CollectionUtils.isNotEmpty(where)) {
            for (Where w : where) {
                if (StringUtils.isNotBlank(w.getField())
                        && w.getField().equals(routingField)
                        && (w.getType().getName().equals(Rel.EQ.getName())
                        || w.getType().getName().equals(Rel.TERM.getName())
                        || w.getType().getName().equals(Rel.MATCH.getName())
                )) {
                    return w.getParam();
                }
                if (CollectionUtils.isNotEmpty(w.getParams()) && isIncludeRoutingField(w.getParams(), routingField) != null) {
                    return isIncludeRoutingField(w.getParams(), routingField);
                }
            }
        }
        return null;
    }

    // elasticsearch 更新使用 painless 脚本进行更新
        /*POST node_object/doc/_update_by_query
        {
            "script": {
            "source": "ctx._source.ds_dzmc = params.ds_dzmc;ctx._source.htbh = params.htbh",
                    "params": {
                "ds_dzmc": 321,
                        "htbh": 157
            },
            "lang": "painless"
        },
            "query": {
            "match": {
                "htbh": 222
            }
        }
        }*/
    @Override
    protected Q transitionUpdateParam(UpdateParamCondition updateParamCondition, D databaseConf, T tableConf) {
        EsHandler esHandler = new EsHandler();
        EsHandler.EsUpdate esUpdate = new EsHandler.EsUpdate();
        esHandler.setEsUpdate(esUpdate);

        // 更新字段
        Map<String, Object> updateParamMap = updateParamCondition.getUpdateParamMap();
        // painless 脚本 Map 对象
        Map<String, Object> updateMap = new LinkedHashMap<>();
        // script
        Map<String, Object> scriptMap = new LinkedHashMap<>();
        // source 拼接
        StringBuilder sourceBuffer = new StringBuilder();
        String sourceFormat = "ctx._source.%s = params.%s";
        String arrayFormatToList = "if(ctx._source.containsKey('$key')){ctx._source.$key.addAll(params.$key)}else{ctx._source.$key=params.$key}";
        String arrayFormatToSet = "if(ctx._source.containsKey('$key')){params.$key.removeAll(ctx._source.$key); if(params.$key.size() > 0){ctx._source.$key.addAll(params.$key)}}else{ctx._source.$key=params.$key}";
        UpdateParamCondition.ArrayProcessMode arrayProcessMode = updateParamCondition.getArrayProcessMode();
        updateParamMap.forEach((key, value) -> {
            String format;
            if (!UpdateParamCondition.ArrayProcessMode.COVER.equals(arrayProcessMode)
                    && (value instanceof List || value.getClass().isArray() && !(value instanceof byte[]))) {
                if (UpdateParamCondition.ArrayProcessMode.ADD_TO_LIST.equals(arrayProcessMode)) {
                    format = StringUtils.replace(arrayFormatToList, "$key", key);
                } else {
                    format = StringUtils.replace(arrayFormatToSet, "$key", key);
                }
            } else {
                format = String.format(sourceFormat, key, key);
            }
            sourceBuffer.append(format).append(";");
        });
        sourceBuffer.deleteCharAt(sourceBuffer.length() - 1);
        scriptMap.put("source", sourceBuffer.toString());
        scriptMap.put("params", updateParamMap);
        scriptMap.put("lang", "painless");
        updateMap.put("script", scriptMap);

        // painless 脚本语句
        String requestJson = JsonIncludeAlwaysUtil.objectToStr(updateMap);

        // 设置更新条件
        List<Where> where = updateParamCondition.getWhere();
        // 查询条件
        Map<String, String> paramMap = new HashMap<>();
        // 查询条件解析
        if (CollectionUtils.isNotEmpty(where)) {
            BurstZone burstZone = new BurstZone();
            parserWhere(where, paramMap, burstZone);
            esUpdate.setStartTime(burstZone.getStartTime());
            esUpdate.setStopTime(burstZone.getStopTime());
            String queryJson = jointQueryJson(paramMap);
            StringBuilder queryBuilder = new StringBuilder(requestJson);
            queryBuilder.deleteCharAt(queryBuilder.length() - 1);
            queryBuilder.append(",\"query\":").append(queryJson).append("}");
            requestJson = queryBuilder.toString();
        }
        esUpdate.setRequestJson(requestJson);
//        esUpdate.setUpdateHttpEntity(new NStringEntity(requestJson, ContentType.APPLICATION_JSON));
        esUpdate.setRefresh(updateParamCondition.getCommitNow());
        return (Q) esHandler;
    }

    @Override
    protected Q transitionInsertParam(AddParamCondition addParamCondition, D databaseConf, T tableConf) {
        EsHandler esHandler = new EsHandler();
        EsHandler.EsInsert esInsert = new EsHandler.EsInsert();
        esHandler.setEsInsert(esInsert);
        esInsert.setCaptureTime(addParamCondition.getCaptureTime());
        esInsert.setRefresh(addParamCondition.getCommitNow());
        List<Map<String, Object>> fieldValueList = addParamCondition.getFieldValueList();
        int docSize = fieldValueList.size();
        esInsert.setDocSize(docSize);

        String routingField = addParamCondition.getRoutingField();
        if (docSize > 1) {
            String index_type = "{\"index\":{\"_index\":\"${indexName}\",\"_type\":\"" + "${docType}";
            StringBuilder bulkBuffer = new StringBuilder();
            for (int i = 0; i < fieldValueList.size(); i++) {
                Map<String, Object> jsonMap = fieldValueList.get(i);
                // 路由判断,字段的值是否符合路由
                String routeString = "";
                if (StringUtils.isNotBlank(routingField) && jsonMap.containsKey(routingField) && isPrimitive(jsonMap.get(routingField))) {
                    routeString = ",\"routing\":\"" + jsonMap.get(routingField) + "\"";
                }
                // 判断是否存在经纬度类型
                transitionValue(jsonMap);
                String idStr = "";
                //idStr 为了加快插入速度,不做为es的id,有需要update或去重的的才需要
                //idStr 是不做为es的 _id
                if (jsonMap.containsKey("md")) {
                    idStr = ",\"_id\":\"" + jsonMap.get("md") + "\"";
                }
                if (jsonMap.containsKey("_id")) {
                    idStr = ",\"_id\":\"" + jsonMap.get("_id") + "\"";
                    jsonMap.remove("_id");
                }
                String json = JsonUtil.objectToStr(jsonMap);
                bulkBuffer.append(index_type + "\"" + idStr + routeString + "}}\n" + json + "\n");
            }
            esInsert.setAddStr(bulkBuffer.toString());
        } else {
            Map<String, Object> map = fieldValueList.get(0);
            if (StringUtils.isNotBlank(routingField) && map.containsKey(routingField) && isPrimitive(map.get(routingField))) {
                esInsert.setRoutingField(routingField);
                esInsert.setRouteInsert(true);
            }
            // 判断是否存在经纬度类型
            transitionValue(map);
            //idStr 为了加快插入速度,不做为es的id,有需要update或去重的的才需要
            //idStr 是不做为es的 _id
            if (map.containsKey("id")) {
                // 为了兼容旧版本中对预处理的支持，添加如下特殊判断
                esInsert.setId("$$$id_" + map.get("id"));
            }
            if (map.containsKey("_id")) {
                esInsert.setId((String) map.get("_id"));
                map.remove("_id");
            }
            esInsert.setAddStr(JsonUtil.objectToStr(map));
        }
        return (Q) esHandler;
    }

    /**
     * 类型判断
     *
     * @param o
     * @return
     */
    private Boolean isPrimitive(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof String) {
            return true;
        }
        try {
            return ((Class<?>) o.getClass().getField("TYPE").get(null)).isPrimitive();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 转换字段类型
     * <p>
     * 注意：以字符串标识经纬度时，维度在前，经度在后，而以数组的方式表示时则 经度在前 维度在后
     *
     * @param map
     */
    private void transitionValue(Map<String, Object> map) {
        Map<String, Object> transitionMap = new HashMap<>(1);
        map.forEach((key, value) -> {
            if (value instanceof Point) {
                Point point = (Point) value;
                transitionMap.put(key, point.getLat() + "," + point.getLon());
            }
        });
        if (MapUtil.isNotEmpty(transitionMap)) {
            map.putAll(transitionMap);
        }
    }

    @Override
    protected Q transitionDeleteParam(DelParamCondition delParamCondition, D databaseConf, T tableConf) {
        EsHandler esHandler = new EsHandler();
        EsHandler.EsDel esDel = new EsHandler.EsDel();
        esHandler.setEsDel(esDel);

        // 设置更新条件
        List<Where> where = delParamCondition.getWhere();
        // 查询条件
        Map<String, String> paramMap = new HashMap<>();
        // 查询条件解析
        String requestJson;
        if (CollectionUtils.isNotEmpty(where)) {
            BurstZone burstZone = new BurstZone();
            parserWhere(where, paramMap, burstZone);
            esDel.setStartTime(burstZone.getStartTime());
            esDel.setStopTime(burstZone.getStopTime());
            String queryJson = jointQueryJson(paramMap);
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("{\"query\":").append(queryJson).append("}");
            requestJson = queryBuilder.toString();
        } else {
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("{\"query\":{").append("\"match_all\": {}}").append("}");
            requestJson = queryBuilder.toString();
        }
        esDel.setRequestJson(requestJson);
        esDel.setRefresh(delParamCondition.getCommitNow());
//        esDel.setQueryHttpEntity(new NStringEntity(requestJson, ContentType.APPLICATION_JSON));
        return (Q) esHandler;
    }

    @Override
    protected Q transitionDropTableParam(DropTableParamCondition dropTableParamCondition, D databaseConf, T tableConf) throws Exception {
        EsHandler esHandler = new EsHandler();
        EsHandler.EsDropTable dropTable = new EsHandler.EsDropTable();
        dropTable.setAlias(dropTableParamCondition.getAlias());
        if (dropTableParamCondition != null) {
            if (StringUtils.isNotBlank(dropTableParamCondition.getStartTime())) {
                dropTable.setStartTime(DateUtil.convertToDate(dropTableParamCondition.getStartTime(), JodaTimeUtil.DEFAULT_YMD_FORMAT));
            }
            if (StringUtils.isNotBlank(dropTableParamCondition.getEndTime())) {
                dropTable.setStopTime(DateUtil.convertToDate(dropTableParamCondition.getEndTime(), JodaTimeUtil.DEFAULT_YMD_FORMAT));
            }
        }
        esHandler.setDropTable(dropTable);
        return (Q) esHandler;
    }

    @Override
    public Q transitionUpsertParam(UpsertParamCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        List<String> conflictFieldList = paramCondition.getConflictFieldList();
        Map<String, Object> upsertParamMap = paramCondition.getUpsertParamMap();
        if (CollectionUtil.isEmpty(conflictFieldList) || !conflictFieldList.contains("_id") || conflictFieldList.size() > 1 || upsertParamMap.get("_id") == null) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "ES 组件进行 upsert 操作有且只能携带 _id 为指定判断是否存在的字段.");
        }
        String id = String.valueOf(upsertParamMap.remove("_id"));
        EsHandler esHandler = new EsHandler();
        EsHandler.EsUpsert esUpsert = new EsHandler.EsUpsert();
        esHandler.setEsUpsert(esUpsert);
        // 设置 _id
        esUpsert.setId(id);
        esUpsert.setCaptureTime(paramCondition.getCaptureTime());
        // 设置是否立即刷新
        esUpsert.setRefresh(paramCondition.getCommitNow());
        // 设置dml
        Map<String, Object> upsertMap = new HashMap<>(2);
        upsertMap.put("doc", upsertParamMap);
        upsertMap.put("doc_as_upsert", true);
        esUpsert.setUpsertStr(JsonIncludeAlwaysUtil.objectToStr(upsertMap));
        return (Q) esHandler;
    }

    @Override
    public Q transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        EsHandler esHandler = new EsHandler();
        EsHandler.EsUpsertBatch esUpsertBatch = new EsHandler.EsUpsertBatch();
        esHandler.setEsUpsertBatch(esUpsertBatch);
        List<String> conflictFieldList = paramCondition.getConflictFieldList();
        List<Map<String, Object>> upsertParamList = paramCondition.getUpsertParamList();
        StringBuilder sb = new StringBuilder();
        for (Map<String, Object> upsertParamMap : upsertParamList) {
            if (CollectionUtil.isEmpty(conflictFieldList) || !conflictFieldList.contains("_id") || conflictFieldList.size() > 1 || upsertParamMap.get("_id") == null) {
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "ES 组件进行 upsert 操作有且只能携带 _id 为指定判断是否存在的字段.");
            }
            String id = String.valueOf(upsertParamMap.remove("_id"));
            String upsertStr = StringUtils.replaceEach(UPSERT_TEMPLATE, new String[]{"${_id}", "${doc}"}, new String[]{id, JsonIncludeAlwaysUtil.objectToStr(upsertParamMap)});
            sb.append(upsertStr);
        }
        esUpsertBatch.setCaptureTime(paramCondition.getCaptureTime());
        // 设置是否立即刷新
        esUpsertBatch.setRefresh(paramCondition.getCommitNow());
        // 设置dml
        esUpsertBatch.setUpsertStr(sb.toString());
        esUpsertBatch.setDocCount(upsertParamList.size());
        return (Q) esHandler;
    }


    private String joinHighlightJson(Map<String, String> highlightMap) {
        String highlight = highlightMap.get("highlight");
        if(StringUtils.isBlank(highlight)) {
            return "";
        }
        return ",\"highlight\": "+highlight;
    }
    /**
     * 拼接 query 部分 json
     *
     * @param paramMap
     * @return
     */
    private String jointQueryJson(Map<String, String> paramMap) {
        if(paramMap.isEmpty()) {
            return "{\"match_all\":{}}";
        }
        return paramMap.get("query");
    }

    /**
     * 拼接 es 查询语句
     *
     * @param fields
     * @param highlightMap
     * @param paramMap
     * @param sortMap
     * @param aggMap
     * @param suggestMap
     * @return
     */
    private String transitionEsRequestJson(List<String> fields, Map<String, String> paramMap, Map<String, String> highlightMap,  Map<String, Sort> sortMap, Map<String, Object> aggMap, boolean isCount, Map<String, Object> suggestMap) {
        StringBuilder selectFields = new StringBuilder("");
        if (null != fields && fields.size() > 0) {
            for (String field : fields) {
                selectFields.append("\"").append(field).append("\",");
            }
            selectFields.delete(selectFields.length() - 1, selectFields.length()).insert(0, "\"_source\":[").append("],\n");
        }
        String _source = selectFields.toString();
        StringBuilder sortBuffer = new StringBuilder("");
        if (null != sortMap && sortMap.size() > 0) {
            for (Map.Entry<String, Sort> entry : sortMap.entrySet()) {
                String field = entry.getKey();
                String orderValue = entry.getValue().toString().toLowerCase();
                sortBuffer.append("{\"").append(field).append("\":{\"order\":\"").append(orderValue).append("\"}},");
            }
            sortBuffer.delete(sortBuffer.length() - 1, sortBuffer.length()).insert(0, ",\n\"sort\":[").append("]");
        }
        String sort = sortBuffer.toString();
        StringBuilder queryBuffer = new StringBuilder();
        String queryJson = jointQueryJson(paramMap);
        if (isCount) {
            queryBuffer.append("{").append(_source).append("\"track_total_hits\": true,").append("\"query\":")
                    .append(queryJson).append(joinHighlightJson(highlightMap)).append(sort).append("}");
        } else {
            queryBuffer.append("{").append(_source).append("\"query\":")
                    .append(queryJson).append(joinHighlightJson(highlightMap)).append(sort).append("}");
        }

        // 不包含聚合的语句
        String queryJsonStr = queryBuffer.toString();

        // 增加对agg查询
        if (aggMap != null) {
            Map<String, Object> queryMap = JsonIncludeAlwaysUtil.jsonStrToMap(queryJsonStr);
            queryMap.put("aggs", aggMap);
            queryJsonStr = JsonUtil.objectToStr(queryMap);
        }

        // 增加推荐语法查询
        if (MapUtil.isNotEmpty(suggestMap)) {
            Map<String, Object> queryMap = JsonIncludeAlwaysUtil.jsonStrToMap(queryJsonStr);
            queryMap.put("suggest", suggestMap);
            queryJsonStr = JsonUtil.objectToStr(queryMap);
        }
        return queryJsonStr;
    }

    /**
     * 对日期格式进行格式化
     * ES 默认是 strict_date_optional_time，则默认转为 yyyy-MM-dd'T'HH:mm:ss.000
     *
     * @param timeStr
     * @return
     */
    private String timeFormat(String timeStr, String timePattern) {
        if (StringUtils.isBlank(timePattern) ||
                (!StringUtils.equalsIgnoreCase(timePattern, JodaTimeUtil.DEFAULT_YMD_FORMAT)
                        && !StringUtils.equalsIgnoreCase(timePattern, JodaTimeUtil.DEFAULT_YMDHMS_FORMAT)
                        && !StringUtils.equalsIgnoreCase(timePattern, JodaTimeUtil.SOLR_TDATE_FORMATE)
                        && !StringUtils.equalsIgnoreCase(timePattern, JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE)
                        && !StringUtils.equalsIgnoreCase(timePattern, JodaTimeUtil.COMPACT_YMDHMS_FORMAT)
                        && !StringUtils.equalsIgnoreCase(timePattern, JodaTimeUtil.COMPACT_YMD_FORMAT)
                        && !StringUtils.equalsIgnoreCase(timePattern, JodaTimeUtil.COMPACT_YMDH_FORMAT))) {
            boolean timeFlag = DateUtil.checkDateString(timeStr, JodaTimeUtil.DEFAULT_YMDHMS_FORMAT);
            if (timeFlag) {
                timeStr = JodaTimeUtil.time(timeStr, JodaTimeUtil.DEFAULT_YMDHMS_FORMAT)
                        .toString(JodaTimeUtil.SOLR_TDATE_FORMATE);
            }
        } else {
            try {
                Date time = DateUtils.parseDateStrictly(timeStr, JodaTimeUtil.DEFAULT_YMD_FORMAT
                        , JodaTimeUtil.DEFAULT_YMDHMS_FORMAT
                        , JodaTimeUtil.SOLR_TDATE_FORMATE
                        , JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE
                        , JodaTimeUtil.COMPACT_YMDHMS_FORMAT
                        , JodaTimeUtil.COMPACT_YMD_FORMAT
                        , JodaTimeUtil.COMPACT_YMDH_FORMAT);
                timeStr = cn.hutool.core.date.DateUtil.format(time, timePattern);
            } catch (Exception e) {
            }
        }
        return timeStr;
    }

    /**
     * 聚合函数解析
     *
     * @param aggs
     * @return
     */
    public Map<String, Object> aggFunctionToEsAggMap(Aggs aggs) {
        List<AggFunction> aggFunctions = aggs.getAggFunctions();


        Map<String, Object> aggFunctionsMap = aggFunctionToEsAggMap(aggFunctions);
        Where having = aggs.getHaving();
        if (having != null){
            Map<String, Object> bucketScript = new HashMap<>(1);
            Map<String, Object> bucketSelector = new HashMap<>(2);
            buildBucketSelector(bucketSelector, Lists.newArrayList(having), aggs.getAggName());
            if (MapUtil.isNotEmpty(bucketSelector)) {
                bucketScript.put("bucket_selector", bucketSelector);
                aggFunctionsMap.put("bucket_script", bucketScript);
            }
        }
        return aggFunctionsMap;
    }

    public Map<String, Object> aggFunctionToEsAggMap(List<AggFunction> aggFunctionList) {
        return aggFunctionList.stream().flatMap(aggFunction -> {
            Map<String, Object> funMap = new HashMap<>(1);
            switch (aggFunction.getAggFunctionType()) {
                case AVG:
                    if (StringUtils.isNoneBlank(aggFunction.getWeightField())) {
                        Map<String, Object> operationMap = new HashMap<>(1);
                        operationMap.put("weighted_" + AggFunctionType.AVG.getType(), MapUtil.builder("value", MapUtil.builder("field", aggFunction.getField()).build()).put("weight", MapUtil.builder("field", aggFunction.getWeightField()).build()).build());
                        funMap.put(aggFunction.getFunctionName(), operationMap);
                    } else {
                        Map<String, Object> operationMap = new HashMap<>(1);
                        operationMap.put(aggFunction.getAggFunctionType().getType(), MapUtil.builder("field", aggFunction.getField()).build());
                        funMap.put(aggFunction.getFunctionName(), operationMap);
                    }
                    break;
                case COUNT:
                    Map<String, Object> countOperationMap = new HashMap<>(1);
                    countOperationMap.put("value_count", MapUtil.builder("field", aggFunction.getField()).build());
                    funMap.put(aggFunction.getFunctionName(), countOperationMap);
                    break;
                case DISTINCT:
                    Map<String, Object> distinctOperationMap = new HashMap<>(1);
                    distinctOperationMap.put("cardinality", MapUtil.builder("field", aggFunction.getField()).build());
                    funMap.put(aggFunction.getFunctionName(), distinctOperationMap);
                    break;
                case PERCENTILES:
                    Map<String, Object> percentilesOperationMap = new HashMap<>(1);
                    Map<String, Object> params = new HashMap<>();
                    params.put("field", aggFunction.getField());
                    params.put("percents", aggFunction.getPercentiles());
                    percentilesOperationMap.put("percentiles", params);
                    funMap.put(aggFunction.getFunctionName(), percentilesOperationMap);
                    break;
                default:
                    Map<String, Object> operationMap = new HashMap<>(1);
                    operationMap.put(aggFunction.getAggFunctionType().getType(), MapUtil.builder("field", aggFunction.getField()).build());
                    funMap.put(aggFunction.getFunctionName(), operationMap);
                    break;
            }
            Integer hitNum = aggFunction.getHitNum();
            if (hitNum != null) {
                funMap.put(aggFunction.getHitName(), MapUtil.builder().put("top_hits", MapUtil.builder("size", hitNum).build()).build());
            }
            return Stream.of(funMap);
        }).reduce((a, b) -> {
            a.putAll(b);
            return a;
        }).orElse(new HashMap<>());
    }

    private void buildBucketSelector(Map<String, Object> bucketSelector, List<Where> bucketSelectorWhereList, String aggName) {
        Map<String,String> buckets_path = new HashMap<>();
        String script = buildScript(buckets_path, Rel.AND, bucketSelectorWhereList, aggName);
        if (script.length() == 0){
            return;
        }
        script = script.substring(0, script.length() - 4);
        script = script.replaceAll("and", "&&").replaceAll("or", "||").replaceAll("not", "!");
        bucketSelector.put("buckets_path",buckets_path);
        bucketSelector.put("script",MapUtil.builder("source",script).build());
    }

    private String buildScript(Map<String, String> bucketsPath, Rel rel, List<Where> bucketSelectorWhereList, String aggName) {
        StringBuilder script = new StringBuilder();
        if (bucketSelectorWhereList != null && !bucketSelectorWhereList.isEmpty()) {
            for (Where where : bucketSelectorWhereList) {
                if (Rel.AND.equals(where.getType()) || Rel.OR.equals(where.getType()) || Rel.NOT.equals(where.getType())) {
                    List<Where> params = where.getParams();
                    String sql = buildScript(bucketsPath, where.getType(), params, aggName);
                    if (StringUtils.isNotBlank(sql)) {
                        script.append(" (");
                        sql = sql.substring(0, sql.length() - 2 - where.getType().getName().length());
                        script.append(sql);
                        script.append(") ");
                        script.append(rel.getName());
                        script.append(" ");
                        if (Rel.NOT.equals(where.getType())) {
                            script.insert(0,where.getType() + " ");
                        }
                    }
                } else if (!StringUtils.equalsIgnoreCase(where.getField(), aggName)) {
                    String whereSql = buildScript(where, rel, bucketsPath);
                    script.append(whereSql);
                }
            }
        }
        return script.toString();
    }

    private String buildScript(Where bucketSelectorWhere, Rel rel, Map<String, String> bucketsPath) {
        StringBuilder sb = new StringBuilder();
        if (whereHandler(bucketSelectorWhere)) {
            String relStrHandler = relStrHandler(bucketSelectorWhere, bucketsPath);
            if (StringUtils.isNotBlank(StringUtils.trimToEmpty(relStrHandler))) {
                sb.append(relStrHandler);
                sb.append(" ");
                sb.append(rel.getName());
                sb.append(" ");
            }
        }
        return sb.toString();
    }

    private boolean whereHandler(Where where) {
        return where != null && StringUtils.isNotBlank(where.getField()) && where.getParam() != null;
    }

    /**
     * 解析操作符
     *
     * @param bucketSelectorWhere
     * @return
     */
    private String relStrHandler(Where bucketSelectorWhere,Map<String, String> bucketsPath) {
        Rel rel = bucketSelectorWhere.getType();
        StringBuilder sb = new StringBuilder();
        sb.append("(").append("params.");
        sb.append(bucketSelectorWhere.getField());
        bucketsPath.put(bucketSelectorWhere.getField(),bucketSelectorWhere.getField());
        Object value = bucketSelectorWhere.getParam();
        switch (rel) {
            case EQ:
                sb.append(" == ");
                sb.append(value);
                break;
            case NE:
                sb.append(" != ");
                sb.append(value);
                break;
            case GT:
                sb.append(" > ");
                sb.append(value);
                break;
            case GTE:
                sb.append(" >= ");
                sb.append(value);
                break;
            case LT:
                sb.append(" < ");
                sb.append(value);
                break;
            case LTE:
                sb.append(" <= ");
                sb.append(value);
                break;
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * 聚合条件解析
     *
     * @param aggs
     * @return
     */
    public Map<String, Object> aggs2esAggMap(Aggs aggs) {
        //不包含aggs eg: agg_name->agg_args
        Map<String, Object> aggMap = new HashMap<>();
        if (StringUtils.isNotBlank(aggs.getSrcAggsParam())) {
            aggMap = JsonUtil.jsonStrToMap(aggs.getSrcAggsParam());
        } else {
            // 若 aggs 对象属性不包含字段和类型，则表示不进行字段聚合，直接统计
            if (StringUtils.isBlank(aggs.getField()) || aggs.getType() == null && CollectionUtil.isNotEmpty(aggs.getAggFunctions())) {
                aggMap = aggFunctionToEsAggMap(aggs);
            } else {
                if (!isAcceptAggType(aggs.getType())) {
                    throw new BusinessException("不支持的agg 类型:" + aggs.getType());
                }
                // 分组
                if (AggOpType.TERMS.equals(aggs.getType()) || AggOpType.GROUP.equals(aggs.getType())) {
                    if (StringUtils.isNotBlank(aggs.getField())) {
                        Map<String, Object> termsMap = new HashMap();
                        Map<String, Object> fieldMap = new HashMap();
                        fieldMap.put("field", aggs.getField());
                        if(StringUtils.isNotBlank(aggs.getExecutionHint())) {
                            fieldMap.put("execution_hint", aggs.getExecutionHint());
                        }
                        if (aggs.getLimit() > 0) {
                            if (aggs.getOffset() != null && aggs.getOffset() > 0) {
                                fieldMap.put("size", aggs.getLimit() + aggs.getOffset());
                            } else {
                                fieldMap.put("size", aggs.getLimit());
                            }
                        } else if (aggs.getLimit() == (-1)) {
                            // 全量太大了，超时不说，很容易内存溢出
                            fieldMap.put("size", 10000);
                        }
                        if (aggs.getOffset() != null && aggs.getOffset() > 0) {
                            fieldMap.put("offset", aggs.getOffset());
                        }
                        if (aggs.getMincount() > 0) {
                            fieldMap.put("min_doc_count", aggs.getMincount());
                        }
                        if (CollectionUtil.isNotEmpty(aggs.getOrders())) {
                            Optional<Map<String, String>> optional = aggs.getOrders().stream().flatMap(order -> Stream.of(MapUtil.builder(StringUtils.equals(aggs.getField(), order.getField()) ? "_key" : order.getField(), order.getSort().getName()).build()))
                                    .reduce((a, b) -> {
                                        a.putAll(b);
                                        return a;
                                    });
                            if(optional.isPresent()) {
                                fieldMap.put("order", optional.get());
                            }
                        } else if (StringUtils.isNotBlank(aggs.getOrder())) {
                            Map<String, Object> orderMap = new HashMap();
                            orderMap.put(aggs.getOrder(), aggs.getOrderType());
                            fieldMap.put("order", orderMap);
                        }
                        termsMap.put(AggOpType.TERMS.getOp(), fieldMap);
                        Map<String, Object> subAggMap = new HashMap<>();
                        if (CollectionUtil.isNotEmpty(aggs.getAggFunctions())) {
                            subAggMap.putAll(aggFunctionToEsAggMap(aggs));
                        }
                        if (CollectionUtil.isNotEmpty(aggs.getAggList())) {
                            subAggMap.putAll(aggs.getAggList().stream().map(agg -> aggs2esAggMap(agg))
                                    .reduce((a, b) -> {
                                        a.putAll(b);
                                        return a;
                                    }).orElse(new HashMap<>()));
                        } else if (aggs.getAggs() != null) {
                            subAggMap.putAll(aggs2esAggMap(aggs.getAggs()));
                        }
                        if (MapUtil.isNotEmpty(subAggMap)) {
                            termsMap.put("aggs", subAggMap);
                        }
                        String aggName = StringUtils.isBlank(aggs.getAggName()) ? "aggs_" + aggs.getType() + "_" + aggs.getField() : aggs.getAggName();
                        aggMap.put(aggName, termsMap);
                    }
                }
                // 直方图
                if (AggOpType.HISTOGRAM.equals(aggs.getType())) {
                    if (StringUtils.isNotBlank(aggs.getField())) {
                        Map<String, Object> dateMap = new HashMap();
                        Map<String, Object> fieldMap = new HashMap();
                        fieldMap.put("field", aggs.getField());
                        if (StringUtils.isNotBlank(aggs.getInterval())) {
                            fieldMap.put("interval", Integer.valueOf(aggs.getInterval()));
                        }
                        if (CollectionUtil.isNotEmpty(aggs.getOrders())) {
                            Optional<Map<String, String>> optional = aggs.getOrders().stream().flatMap(order -> Stream.of(MapUtil.builder(StringUtils.equals(aggs.getField(), order.getField()) ? "_key" : order.getField(), order.getSort().getName()).build()))
                                    .reduce((a, b) -> {
                                        a.putAll(b);
                                        return a;
                                    });
                            if(optional.isPresent()) {
                                fieldMap.put("order", optional.get());
                            }
                        } else if (StringUtils.isNotBlank(aggs.getOrder())) {
                            Map<String, Object> orderMap = new HashMap();
                            orderMap.put(aggs.getOrder(), aggs.getOrderType());
                            fieldMap.put("order", orderMap);
                        }
                        dateMap.put(AggOpType.HISTOGRAM.getOp(), fieldMap);
                        if (aggs.getAggs() != null) {
                            dateMap.put("aggs", aggs2esAggMap(aggs.getAggs()));
                        }
                        String aggName = StringUtils.isBlank(aggs.getAggName()) ? "aggs_" + aggs.getType() + "_" + aggs.getField() : aggs.getAggName();
                        aggMap.put(aggName, dateMap);
                    }
                }
                // 日期直方图
                if (AggOpType.DATE_HISTOGRAM.equals(aggs.getType())) {
                    if (StringUtils.isNotBlank(aggs.getField())) {
                        Map<String, Object> dateMap = new HashMap();
                        Map<String, Object> fieldMap = new HashMap();
                        fieldMap.put("field", aggs.getField());
                        if (StringUtils.isNotBlank(aggs.getInterval())) {
                            fieldMap.put("interval", aggs.getInterval());
                        }
                        if (StringUtils.isNotBlank(aggs.getFormat())) {
                            fieldMap.put("format", aggs.getFormat());
                        }
                        if (aggs.getMincount() > 0) {
                            fieldMap.put("min_doc_count", aggs.getMincount());
                        }
                        if (CollectionUtil.isNotEmpty(aggs.getOrders())) {
                            Optional<Map<String, String>> optional = aggs.getOrders().stream().flatMap(order -> Stream.of(MapUtil.builder(StringUtils.equals(aggs.getField(), order.getField()) ? "_key" : order.getField(), order.getSort().getName()).build()))
                                    .reduce((a, b) -> {
                                        a.putAll(b);
                                        return a;
                                    });
                            if(optional.isPresent()) {
                                fieldMap.put("order", optional.get());
                            }
                        } else if (StringUtils.isNotBlank(aggs.getOrder())) {
                            Map<String, Object> orderMap = new HashMap();
                            orderMap.put(aggs.getOrder(), aggs.getOrderType());
                            fieldMap.put("order", orderMap);
                        }
                        // 过滤条件
                        Where having = aggs.getHaving();
                        if (having != null) {
                            List<Where> dateHistogramHaving = getDateHistogramHaving(having, aggs.getAggName());
                            Map paramMap = null;
                            if (CollectionUtil.isNotEmpty(dateHistogramHaving)) {
                                paramMap = dateHistogramHaving.stream().flatMap(where -> {
                                    switch (where.getType()) {
                                        case LT:
                                        case LTE:
                                            return Stream.of(MapUtil.builder("max", where.getParam()).build());
                                        case GT:
                                        case GTE:
                                            return Stream.of(MapUtil.builder("min", where.getParam()).build());
                                        default:
                                            return Stream.of(com.meiya.whalex.util.collection.MapUtil.EMPTY_MAP);
                                    }
                                }).reduce((a, b) -> {
                                    a.putAll(b);
                                    return a;
                                }).orElse(new HashMap());
                            }
                            if (MapUtil.isNotEmpty(paramMap)) {
                                DateHistogramBoundsType dateHistogramBoundsType = aggs.getDateHistogramBoundsType();
                                if (dateHistogramBoundsType == null) {
                                    dateHistogramBoundsType = DateHistogramBoundsType.EXTENDED_BOUNDS;
                                }
                                if (DateHistogramBoundsType.HARD_BOUNDS.equals(dateHistogramBoundsType)) {
                                    fieldMap.put("hard_bounds", paramMap);
                                } else {
                                    fieldMap.put("extended_bounds", paramMap);
                                }
                            }
                        }
                        dateMap.put(AggOpType.DATE_HISTOGRAM.getOp(), fieldMap);
                        if (CollectionUtil.isNotEmpty(aggs.getAggFunctions())) {
                            dateMap.put("aggs", aggFunctionToEsAggMap(aggs));
                        } else if (aggs.getAggs() != null) {
                            dateMap.put("aggs", aggs2esAggMap(aggs.getAggs()));
                        }
                        String aggName = StringUtils.isBlank(aggs.getAggName()) ? "aggs_" + aggs.getType() + "_" + aggs.getField() : aggs.getAggName();
                        aggMap.put(aggName, dateMap);
                    }
                }
                // 嵌套聚合
                if (AggOpType.NESTED.equals(aggs.getType())) {
                    if (StringUtils.isNotBlank(aggs.getField())) {
                        Map<String, Object> nestedMap = new HashMap();
                        Map<String, Object> fieldMap = new HashMap();
                        fieldMap.put("path", aggs.getField());
                        nestedMap.put(AggOpType.NESTED.getOp(), fieldMap);
                        Map<String, Object> subAggMap = new HashMap<>();
                        if (CollectionUtil.isNotEmpty(aggs.getAggFunctions())) {
                            subAggMap.putAll(aggFunctionToEsAggMap(aggs));
                        } else if (CollectionUtil.isNotEmpty(aggs.getAggList())) {
                            subAggMap.putAll(aggs.getAggList().stream().map(agg -> aggs2esAggMap(agg))
                                    .reduce((a, b) -> {
                                        a.putAll(b);
                                        return a;
                                    }).orElse(new HashMap<>()));
                        } else if (aggs.getAggs() != null) {
                            subAggMap.putAll(aggs2esAggMap(aggs.getAggs()));
                        }
                        if (MapUtil.isNotEmpty(subAggMap)) {
                            nestedMap.put("aggs", subAggMap);
                        }
                        String aggName = StringUtils.isBlank(aggs.getAggName()) ? "aggs_" + aggs.getType() + "_" + aggs.getField() : aggs.getAggName();
                        aggMap.put(aggName, nestedMap);
                    }
                }
                // 排重聚合
                if (AggOpType.DISTINCT.equals(aggs.getType())) {
                    if (StringUtils.isNotBlank(aggs.getField())) {
                        Map<String, Object> distinctMap = new HashMap();
                        Map<String, Object> fieldMap = new HashMap();
                        fieldMap.put("field", aggs.getField());
                        distinctMap.put("cardinality", fieldMap);
                        String aggName = StringUtils.isBlank(aggs.getAggName()) ? "aggs_" + aggs.getType() + "_" + aggs.getField() : aggs.getAggName();
                        aggMap.put(aggName, distinctMap);
                    }
                }
                // 自定义范围聚合
                if (AggOpType.RANGE.equals(aggs.getType())) {
                    if (StringUtils.isNotBlank(aggs.getField())) {
                        Map<String, Object> rangeMap = new HashMap<>(1);
                        Map<String, Object> range = new HashMap<>();
                        rangeMap.put("range", range);
                        range.put("field", aggs.getField());
                        range.put("ranges", aggs.getRangeKeys());
                        aggMap.put(aggs.getAggName(), rangeMap);
                        Map<String, Object> subAggMap = new HashMap<>();
                        if (CollectionUtil.isNotEmpty(aggs.getAggFunctions())) {
                            subAggMap.putAll(aggFunctionToEsAggMap(aggs));
                        } else if (CollectionUtil.isNotEmpty(aggs.getAggList())) {
                            subAggMap.putAll(aggs.getAggList().stream().map(agg -> aggs2esAggMap(agg))
                                    .reduce((a, b) -> {
                                        a.putAll(b);
                                        return a;
                                    }).orElse(new HashMap<>()));
                        } else if (aggs.getAggs() != null) {
                            subAggMap.putAll(aggs2esAggMap(aggs.getAggs()));
                        }
                        if (MapUtil.isNotEmpty(subAggMap)) {
                            rangeMap.put("aggs", subAggMap);
                        }
                    }
                }
                // 若存在 hit 则返回参与聚合的数据
                AggHit aggHit = aggs.getAggHit();
                Integer hitNum = aggs.getHitNum();
                if (aggHit != null) {
                    Integer size = aggHit.getSize();
                    List<Order> orders = aggHit.getOrders();
                    List<String> select = aggHit.getSelect();
                    MapBuilder<Object, Object> builder = MapUtil.builder("size", size);
                    if (CollectionUtil.isNotEmpty(orders)) {
                        Optional<Map<String, String>> optional = orders.stream().flatMap(order -> Stream.of(MapUtil.builder(order.getField(), order.getSort().getName()).build()))
                                .reduce((a, b) -> {
                                    a.putAll(b);
                                    return a;
                                });
                        if(optional.isPresent()) {
                            builder.put("sort", optional.get());
                        }
                    }
                    if (CollectionUtil.isNotEmpty(select)) {
                        select = new ArrayList<>(select);
                        Iterator<String> iterator = select.iterator();
                        while (iterator.hasNext()) {
                            String field = iterator.next();
                            if (("_id".equals(field))) {
                                iterator.remove();
                            }
                        }
                        // 若未显示指定 查询结果包含id，则需要默认带上
                        if (!select.contains("id")) {
                            select.add("id");
                        }

                        Map<String, List<String>> includes = MapUtil.builder("includes", select).build();
                        builder.put("_source", includes);
                    }
                    aggMap.put(aggs.getHitName(), MapUtil.builder().put("top_hits", builder.build()).build());
                } else if (hitNum != null) {
                    aggMap.put(aggs.getHitName(), MapUtil.builder().put("top_hits", MapUtil.builder("size", hitNum).build()).build());
                }
            }
        }
        return aggMap;
    }

    /**
     * 从 HAVING 中提取针对 Date_Histogram 聚合的过滤条件
     *
     * @param having
     * @return
     */
    private List<Where> getDateHistogramHaving(Where having, String aggName) {

        if (having == null) {
            return null;
        }

        Rel type = having.getType();
        String field = having.getField();

        if (Rel.AND.equals(type) || Rel.OR.equals(type)) {
            List<Where> params = having.getParams();
            if (CollectionUtil.isNotEmpty(params)) {
                List<Where> dateHistogramHaving = new ArrayList<>();
                for (Where param : params) {
                    List<Where> whereList = getDateHistogramHaving(param, aggName);
                    if (CollectionUtil.isNotEmpty(whereList)) {
                        dateHistogramHaving.addAll(whereList);
                    }
                }
                return dateHistogramHaving;
            } else {
                return null;
            }
        } else if (StringUtils.equalsIgnoreCase(field, aggName)) {
            return CollectionUtil.newArrayList(having);
        } else {
            return null;
        }
    }

    /**
     * 只支持 {@link AggOpType} 里的类型，其它不支持
     *
     * @param type
     * @return
     */
    private boolean isAcceptAggType(AggOpType type) {
        if (type == null) {
            return false;
        }
        if (AggOpType.GROUP.equals(type) || AggOpType.TERMS.equals(type) || AggOpType.DATE_HISTOGRAM.equals(type) || AggOpType.NESTED.equals(type) || AggOpType.HISTOGRAM.equals(type) || AggOpType.DISTINCT.equals(type) || AggOpType.RANGE.equals(type)) {
            return true;
        }
        return false;
    }

    /**
     * 设置业务时间
     * @param wheres
     * @param inBz
     * @param outBz
     */
    private void setBurstZone(List<Where> wheres, BurstZone inBz, BurstZone outBz) {
        if(inBz != null) {
            outBz.setStartTime(inBz.getStartTime());
            outBz.setStopTime(inBz.getStopTime());
        }
    }
    /**
     * 解析 where 实体，转换为 es 语法，并设置分表时间
     *
     * @param where
     * @param paramMap
     * @param burstZone
     */
    private void parserWhere(List<Where> where, Map<String, String> paramMap, BurstZone burstZone) {

        //数据预处理
        List<Where> wheres = preHandWhereList(where, Rel.AND);

        //获取业务时间
        BurstZone bz = getBursZone(wheres);
        //设置时间分片
        setBurstZone(wheres, bz, burstZone);

        //解析成es查询语法
        String whereStr = parserWhereEntrance(wheres);
        paramMap.put("query", whereStr);

    }

    /**
     * 获取业务字段
     * @param wheres
     * @return
     */
    private BurstZone getBursZone(List<Where> wheres) {
        BurstZone burstZone = null;
        Iterator<Where> iterator = wheres.iterator();
        while (iterator.hasNext()) {
            Where where = iterator.next();
            List<Where> params = where.getParams();
            if (CollectionUtils.isNotEmpty(params)) {
                burstZone = getBursZone(params);
                if (burstZone != null && StringUtils.isNoneBlank(burstZone.getStartTime(), burstZone.getStopTime())) {
                    return burstZone;
                }
            } else {
                String field = where.getField();
                if (CommonConstant.BURST_ZONE.equals(field)) {
                    if (burstZone == null) {
                        burstZone = new BurstZone();
                    }
                    Rel type = where.getType();
                    if (Rel.BETWEEN.equals(type)) {
                        List param = (List) where.getParam();
                        burstZone.setStartTime(param.get(0).toString());
                        burstZone.setStopTime(param.get(1).toString());
                        iterator.remove();
                        return burstZone;
                    } else {
                        Object param = where.getParam();
                        if (Rel.LTE.equals(type) || Rel.LT.equals(type)) {
                            burstZone.setStopTime(param.toString());
                            iterator.remove();
                        } else if (Rel.GT.equals(type) || Rel.GTE.equals(type)) {
                            burstZone.setStartTime(param.toString());
                            iterator.remove();
                        }
                        if (StringUtils.isNoneBlank(burstZone.getStartTime(), burstZone.getStopTime())) {
                            return burstZone;
                        }
                    }
                }
            }
        }
        return burstZone;
    }

    /**
     * 分表时间
     */
    private static class BurstZone {
        private String startTime;
        private String stopTime;

        public String getStartTime() {
            return startTime;
        }

        public void setStartTime(String startTime) {
            this.startTime = startTime;
        }

        public String getStopTime() {
            return stopTime;
        }

        public void setStopTime(String stopTime) {
            this.stopTime = stopTime;
        }
    }

    /**
     * 获取时间条件字段
     * @param wheres
     * @return
     */
    private List<Where> getTimeField(List<Where> wheres) {
        List<Where> timeFieldList = new ArrayList<>();
        for (Where where : wheres) {
            if(where.getType() == Rel.LTE || where.getType() == Rel.LT
                    || where.getType() == Rel.GTE || where.getType() == Rel.GT
                    || where.getType() == Rel.BETWEEN) {
                timeFieldList.add(where);
            }else {
                List<Where> params = where.getParams();
                if(CollectionUtils.isNotEmpty(params)) {
                    timeFieldList.addAll(getTimeField(params));
                }
            }
        }
        return timeFieldList;
    }

    // 逻辑条件解析(and or)
    private String buildLogicCondition(String field, Where where) {
        StringBuilder condition = new StringBuilder();
        Rel type = where.getType();
        if(type == Rel.OR) {
            condition.append("{\"bool\":{\"should\":[");
        } else if (type == Rel.NOT) {
            condition.append("{\"bool\":{\"must_not\":[");
        } else {
            condition.append("{\"bool\":{\"must\":[");
        }
        StringBuilder whereStr = new StringBuilder();
        List<Where> params = where.getParams();
        if(CollectionUtils.isNotEmpty(params)) {
            for (Where param : params) {
                Rel paramType = param.getType();
                String paramCondition = "";
                if(paramType == Rel.OR || paramType == Rel.AND || paramType == Rel.NOT) {
                    paramCondition = buildLogicCondition(field, param);
                }else {
                    paramCondition = buildArithmeticCondition(field, param);
                }
                if(StringUtils.isNotBlank(paramCondition)) {
                    if(whereStr.length() > 0) {
                        whereStr.append(",");
                    }
                    whereStr.append(paramCondition);
                }
            }
        }

        condition.append(whereStr.toString()).append("]}}");
        return condition.toString();
    }

    // 算术条件解析
    private String buildArithmeticCondition(String parentField, Where where) {
        String field = where.getField();

        if(StringUtils.isBlank(field)) {
            return "";
        }

        if(parentField != null && !field.startsWith(parentField + ".")) {
            field = parentField + "." + field;
        }

        switch (where.getType()) {
            case LT:
            case LTE:
            case GT:
            case GTE:
            case BETWEEN:
                return buildRange(field, where.getParam(), where.getBoost(), where.getType());
            case NESTED:
                if (where.getParams() instanceof List) {
                    // 嵌套查询的条件字段需要填写engineDsls.resourceId这种类型
                    String queryJson = _parserWhereEntrance(field, where.getParams());
                    return "{\"nested\":{\"path\":\"" + where.getField() + "\",\"query\":" + queryJson + "}}";
                }
                break;
            case NE:
                Object val = where.getParam();
                if (val instanceof String) {
                    val = timeFormat(val.toString(), where.getParamFormat());
                }
                // 不等于,加上字段存在的条件
                return "{\"bool\":{\"must_not\":["+_buildMatch(field, val, where.getBoost())+"]}}";
            case IN:
                Object[] inParams = null;
                if (where.getParam() instanceof List) {
                    inParams = ((List) where.getParam()).toArray(new Object[1]);
                } else {
                    inParams = ((String) where.getParam()).replace("[", "").replace("]", "").split(",");
                }
                StringBuilder inFields_value = new StringBuilder();
                if (null != inParams && inParams.length > 0) {
                    for (int i = 0; i < inParams.length; i++) {
                        if (i != inParams.length - 1) {
                            inFields_value.append("\"").append(inParams[i]).append("\",");
                        } else {
                            inFields_value.append("\"").append(inParams[i]).append("\"");
                        }
                    }
                }
                //如果是根据id查,兼容旧模型,根据 _source:id 和 _id 一起查
                if ("id".equals(field)) {
                    return "{\"bool\":{\"should\":["+buildTerms(field, inFields_value.toString(), where.getBoost())+","+buildTerms("_id", inFields_value.toString(), where.getBoost())+"]}}";
                } else {
                    return buildTerms(field, inFields_value.toString(), where.getBoost());
                }
            case NIN:
                String[] ninParams = null;
                if (where.getParam() instanceof List) {
                    ninParams = (String[]) ((List) where.getParam()).toArray(new String[1]);
                } else {
                    ninParams = ((String) where.getParam()).replace("[", "").replace("]", "").split(",");
                }
                StringBuilder ninFields_value = new StringBuilder();
                if (null != ninParams && ninParams.length > 0) {
                    for (int i = 0; i < ninParams.length; i++) {
                        if (i != ninParams.length - 1) {
                            ninFields_value.append("\"").append(ninParams[i]).append("\",");
                        } else {
                            ninFields_value.append("\"").append(ninParams[i]).append("\"");
                        }
                    }
                }
                return "{\"bool\":{\"must_not\":["+buildTerms(field, ninFields_value.toString(), where.getBoost())+"]}}";
            case NULL:
                return "{\"bool\":{\"must_not\":["+buildExists(field, where.getBoost())+"]}}";
            case EXISTS:
            case NOT_NULL:
                return buildExists(field, where.getBoost());
            case NOT_FRONT_LIKE:
                return "{\"bool\":{\"must_not\":["+buildWildcard(field, "*" + where.getParam(), where.getBoost())+"]}}";
            case NOT_TAIL_LIKE:
                return "{\"bool\":{\"must_not\":["+buildWildcard(field, where.getParam() + "*", where.getBoost())+"]}}";
            case NOT_MIDDLE_LIKE:
                if (where.getParam() != null) {
                    String replaceEach = StringUtils.replaceEach(where.getParam().toString(), new String[]{"%"}, new String[]{"*"});
                    return "{\"bool\":{\"must_not\":["+buildWildcard(field, replaceEach, where.getBoost())+"]}}";
                } else {
                    return "{\"bool\":{\"must_not\":["+buildWildcard(field, where.getParam(), where.getBoost())+"]}}";
                }
            case FRONT_LIKE:
                return buildWildcard(field, "*" + where.getParam(), where.getBoost());
            case TAIL_LIKE:
                return buildWildcard(field, where.getParam() + "*", where.getBoost());
            case ILIKE:
            case MIDDLE_LIKE:
            case LIKE:
                String likeParam = String.valueOf(where.getParam());
                if (StringUtils.containsIgnoreCase(likeParam, "%")
                        || StringUtils.containsIgnoreCase(likeParam, "*")
                        || StringUtils.containsIgnoreCase(likeParam, "?")) {
                    if (StringUtils.containsIgnoreCase(likeParam, "%")) {
                        likeParam = StringUtils.replace(likeParam, "%", "*");
                    }
                    return buildWildcard(field, likeParam, where.getBoost());
                } else {
                    return buildMatchPhrase(field, likeParam, where.getBoost());
                }
            case ONLY_LIKE:
                return buildWildcard(field, where.getParam(), where.getBoost());
            case MATCH:
                //如果是根据id查,兼容旧模型,根据 _source:id 和 _id 一起查
                return buildMatch(field, where.getParam(), where.getBoost(),  where.getParamFormat());
            case MULTI_MATCH:
                return buildMultiMatch(field, where.getParam(), where.getQueryType(),  where.getOperator());
            case TERM:
                Object value = where.getParam();
                if (value instanceof String) {
                    value = timeFormat(value.toString(), where.getParamFormat());
                }
                return buildTerm(field, value, where.getBoost());
            case MATCH_PHRASE:
                return buildMatchPhrase(field, String.valueOf(where.getParam()), where.getBoost());
            case REGEX:
                return buildRegex(field, where.getParam(), where.getBoost());
            case GEO_DISTANCE:
                /** 地理位置查询, 圆形,对应的paramValue是Map对象，至少有两个参数，
                 * distance;//eg:distance:10km 半径
                 * loc;loc:"23.04805756,113.27598616"//圆点的中心值
                 * 如果不带单位，默认为m
                 */
                if (where.getParam() instanceof Map) {
                    JsonMap geoDistanceMap = new JsonMap();
                    Map<String, Object> paramMap = (Map) where.getParam();
                    String loc = ObjectUtils.toString(paramMap.get(CommonConstant.LOC));
                    String[] split = loc.split(CommonConstant.COMMA);
                    double lon = Double.valueOf(split[0]);
                    double lat = Double.valueOf(split[1]);
                    Map<String, Object> locMap = new HashMap<>(2);
                    locMap.put("lat", lat);
                    locMap.put("lon", lon);
                    JsonMap jsonMap = geoDistanceMap.addSubMap(GEO_DISTANCE.getName());
                    jsonMap.addObject("distance", paramMap.get("distance"))
                            .addObject(field, locMap);
                    if(where.getBoost() != 1) {
                        jsonMap.addObject("boost", where.getBoost());
                    }
                    return JsonUtil.objectToStr(geoDistanceMap);
                }
                break;
            case GEO_BOX:
                /**
                 * 地理位置查询,长方形
                 * 对应的paramValue是Map对象，至少有两个参数，
                 * topLeft;//左上角坐标
                 * bottomReight;//右下角坐标
                 */
                if (where.getParam() instanceof Map) {
                    JsonMap geoBoxMap = new JsonMap();
                    Map<String, Object> paramMap = (Map) where.getParam();
                    String topLeft = ObjectUtils.toString(paramMap.get("topLeft"));
                    String[] topLeftSplit = topLeft.split(CommonConstant.COMMA);
                    double topLeftLon = Double.valueOf(topLeftSplit[0]);
                    double topLeftLat = Double.valueOf(topLeftSplit[1]);
                    Map<String, Object> topLeftMap = new HashMap<>(2);
                    topLeftMap.put("lat", topLeftLat);
                    topLeftMap.put("lon", topLeftLon);
                    String bottomRight = ObjectUtils.toString(paramMap.get("bottomRight"));
                    String[] bottomRightSplit = bottomRight.split(CommonConstant.COMMA);
                    double bottomRightLon = Double.valueOf(bottomRightSplit[0]);
                    double bottomRightLat = Double.valueOf(bottomRightSplit[1]);
                    Map<String, Object> bottomRightMap = new HashMap<>(2);
                    bottomRightMap.put("lat", bottomRightLat);
                    bottomRightMap.put("lon", bottomRightLon);
                    JsonMap jsonMap = geoBoxMap.addSubMap(Rel.GEO_BOX.getName());
                    jsonMap.addSubMap(field)
                            .addObject("top_left", topLeftMap)
                            .addObject("bottom_right", bottomRightMap);
                    if(where.getBoost() != 1) {
                        jsonMap.addObject("boost", where.getBoost());
                    }
                    return JsonUtil.objectToStr(geoBoxMap);
                }
                break;
            case GEO_POLYGON:
                if (where.getParam() instanceof List) {
                    List<Map<String, Object>> paramMap = (List<Map<String, Object>>) where.getParam();
                    JsonMap geoPolygonMap = new JsonMap();
                    JsonMap jsonMap = geoPolygonMap.addSubMap(Rel.GEO_POLYGON.getName());
                    jsonMap.addSubMap(field)
                            .addObject("points", paramMap);
                    if(where.getBoost() != 1) {
                        jsonMap.addObject("boost", where.getBoost());
                    }
                    return JsonUtil.objectToStr(geoPolygonMap);
                }
                break;
            case LENGTH:
                StringBuilder queryBuffer = new StringBuilder();
                queryBuffer.append("{\"exists\":{\"field\":\"").append(field).append("\"}},\n");
                queryBuffer.append("{\"script\":{\"").append("script\":{");
//                switch (type) {
//                    case EQ:
//                        mustQueryBuffer.append("\"source\":\"").append("doc['").append(field).append("'].value.length()==").append(where.getParam()).append("\",\n");
//                        break;
//                    case NE:
//                        mustQueryBuffer.append("\"source\":\"").append("doc['").append(field).append("'].value.length()!=").append(where.getParam()).append("\",\n");
//                        break;
//                    case GT:
//                        mustQueryBuffer.append("\"source\":\"").append("doc['").append(field).append("'].value.length()>").append(where.getParam()).append("\",\n");
//                        break;
//                    case GTE:
//                        mustQueryBuffer.append("\"source\":\"").append("doc['").append(field).append("'].value.length()>=").append(where.getParam()).append("\",\n");
//                        break;
//                    case LT:
//                        mustQueryBuffer.append("\"source\":\"").append("doc['").append(field).append("'].value.length()<").append(where.getParam()).append("\",\n");
//                        break;
//                    case LTE:
//                        mustQueryBuffer.append("\"source\":\"").append("doc['").append(field).append("'].value.length()<=").append(where.getParam()).append("\",\n");
//                        break;
//                }
//                mustQueryBuffer.append("\"lang\":").append("\"painless\"").append("}}},\n");
                break;
            case PARENT_ID:
                // 不等于,加上字段存在的条件
                return "{\"parent_id\":{\"type\":\""+ field + "\", \"id\": \"" + where.getParam() + "\"}}";
            case HAS_PARENT:
                String queryJson = _parserWhereEntrance(null, where.getParams());
                return "{\"has_parent\":{\"parent_type\":\"" + where.getField() + "\",\"query\":" + queryJson + "}}";
            case HAS_CHILD:
                return "{\"has_child\":{\"type\":\"" + where.getField() + "\",\"query\":" + _parserWhereEntrance(null, where.getParams()) + "}}";
            default:
                //如果是根据id查,兼容旧模型,根据 _source:id 和 _id 一起查
                return buildMatch(field, where.getParam(), where.getBoost(),  where.getParamFormat());
        }
        return "";
    }

    private String buildMultiMatch(String field, Object param, String queryType, String operator) {

        StringBuilder multiMatch = new StringBuilder();

        multiMatch.append("\"query\": ").append("\"").append(param).append("\"");

        String[] split = field.split(",");
        StringBuilder fields = new StringBuilder();
        fields.append("\"").append(split[0].trim()).append("\"");
        for (int i = 1; i < split.length; i++) {
            fields.append(", ").append("\"").append(split[i].trim()).append("\"");
        }

        multiMatch.append(", ").append("\"fields\": ").append("[").append(fields.toString()).append("]");

        if(StringUtils.isNotBlank(operator)) {
            multiMatch.append(", ").append("\"operator\": ").append("\"").append(operator).append("\"");
        }

        if(StringUtils.isNotBlank(queryType)) {
            multiMatch.append(", ").append("\"type\": ").append("\"").append(queryType).append("\"");
        }

        return "{\"multi_match\":{"+multiMatch.toString()+"}}";
    }

    private String buildRegex(String field, Object param, int boost) {
        if(boost == 1)  {
            return "{\"regexp\":{\""+field+"\":\""+param+"\"}}";
        }
        return "{\"regexp\":{\""+field+"\":{\"value\":\""+param+"\",\"boost\":"+boost+"}}}";
    }

    private String buildExists(String field, int boost) {
        if(boost == 1)  {
            return "{\"exists\":{\"field\":\""+field+"\"}}";
        }
        return "{\"exists\":{\"field\":\""+field+"\",\"boost\":"+boost+"}}";
    }

    private String buildWildcard(String field, Object value, int boost) {
        if(boost == 1)  {
            return "{\"wildcard\":{\""+field+"\":\""+value+"\"}}";
        }
        return "{\"wildcard\":{\""+field+"\":{\"value\":\""+value+"\",\"boost\":"+boost+"}}}";
    }

    private String buildTerms(String field, String inFields_value, int boost) {
        if(boost == 1)  {
            return "{\"terms\":{\"" + field + "\":[" + inFields_value + "]}}";
        }
        return "{\"terms\":{\"" + field + "\":[" + inFields_value + "], \"boost\":"+boost+"}}";
    }

    private String buildTerm(String field, Object value, int boost) {
        if(boost == 1)  {
            return "{\"term\":{\""+field+"\":\""+value+"\"}}";
        }
        return "{\"term\":{\""+field+"\":{\"value\":\""+value+"\",\"boost\":"+boost+"}}}";
    }

    private String buildRange(String field, Object value, int boost, Rel type) {

        String boostStr = "";
        if(boost != 1) {
            boostStr = ", \"boost\": "  + boost;
        }

        if(type.equals(Rel.BETWEEN)) {
            Object[] rangeParams = null;
            if (value instanceof List) {
                rangeParams = ((List) value).toArray();
            } else {
                rangeParams = ((String) value).replace("[", "").replace("]", "").split(",");
            }
            return "{\"range\":{\""+field+"\":{\"gte\":\""+rangeParams[0]+"\",\"lte\":\""+rangeParams[1]+"\""+boostStr+"}}}";
        }

        return "{\"range\":{\"" + field + "\":{\""+type.getName()+"\":\"" + value + "\""+boostStr+"}}}";
    }

    private String buildMatchPhrase(String field, Object value, int boost) {
        if(boost == 1)  {
            return "{\"match_phrase\":{\""+field+"\":\""+value+"\"}}";
        }
        return "{\"match_phrase\":{\""+field+"\":{\"query\":\""+value+"\",\"boost\":"+boost+"}}}";
    }

    private String buildMatch(String field, Object value, int boost, String paramFormat) {
        if ("id".equals(field)) {
            return "{\"bool\":{\"should\":["+_buildMatch(field, value, boost)+","+_buildMatch("_id", value, boost)+"]}}";
        } else {
            if (value instanceof String) {
                value = timeFormat(value.toString(), paramFormat);
            }
            return _buildMatch(field, value, boost);
        }
    }

    private String _buildMatch(String field, Object value, int boost) {
        if(boost == 1)  {
            return "{\"match\":{\""+field+"\":\""+value+"\"}}";
        }
        return "{\"match\":{\""+field+"\":{\"query\":\""+value+"\",\"boost\":"+boost+"}}}";
    }
    /**
     * 数据预处理
     *   1) 进行时间格式化处理
     *   2) 区间查询转化
     *   3) 操作符默认值
     * @param whereList
     * @param rel
     * @return
     */
    private List<Where> preHandWhereList(List<Where> whereList, Rel rel) {
        if(CollectionUtils.isEmpty(whereList)) {
            return whereList;
        }
        List<Where> rangeList = new ArrayList<>();
        Map<String, Where> fieldMapWhere = new HashMap<>();
        for (Where where : whereList) {
            List<Where> params = where.getParams();
            if(CollectionUtils.isNotEmpty(params)) {
                Rel type = where.getType();
                //where.getParams() 说明是逻辑运算，没有type,默认为 Rel.AND
                if(type == null)  {
                    where.setType(Rel.AND);
                }
                where.setParams(preHandWhereList(params, where.getType()));
            }else {
                Object param = where.getParam();
                if(param instanceof String) {
                    // 进行时间格式化处理
                    param = timeFormat(param.toString(),  where.getParamFormat());
                    where.setParam(param);
                }
                // 区间查询转化
                if(rel == Rel.AND) {
                    if(where.getType() == Rel.GTE || where.getType() == Rel.LTE) {
                        Where rangeWhere = fieldMapWhere.get(where.getField());
                        if(rangeWhere == null)  {
                            fieldMapWhere.put(where.getField(), where);
                        }else{
                            Where betweenWhere = new Where();
                            int boost = rangeWhere.getBoost();
                            if(boost < where.getBoost()) {
                                boost = where.getBoost();
                            }
                            betweenWhere.setBoost(boost);
                            betweenWhere.setType(Rel.BETWEEN);
                            betweenWhere.setField(where.getField());
                            Object min = null;
                            Object max = null;
                            if(where.getType() == Rel.LTE) {
                                max = where.getParam();
                            }
                            if(rangeWhere.getType() == Rel.LTE) {
                                max = rangeWhere.getParam();
                            }
                            if(where.getType() == Rel.GTE) {
                                min = where.getParam();
                            }
                            if(rangeWhere.getType() == Rel.GTE) {
                                min = rangeWhere.getParam();
                            }
                            betweenWhere.setParam(Arrays.asList(min, max));
                            rangeList.add(betweenWhere);
                        }
                    }else if(where.getType() == Rel.BETWEEN) {
                        Object whereParam = where.getParam();

                        if(whereParam instanceof List) {
                            where.setParam(whereParam);
                        }else {
                            String[] rangeParams = null;
                            if (whereParam instanceof String) {
                                rangeParams = ((String) whereParam).replace("[", "").replace("]", "").split(",");
                            } else {
                                rangeParams = (String[]) whereParam;
                            }

                            if (rangeParams != null) {
                                List<String> collect = Arrays.stream(rangeParams).collect(Collectors.toList());
                                where.setParam(collect);
                            }
                        }
                    }
                }
            }

        }

        // 移除区间查询原始条件
        for (Where where : rangeList) {
            String field = where.getField();
            whereList = whereList.stream().filter(item -> {
                return !field.equals(item.getField());
            }).collect(Collectors.toList());
        }
        // 加入区间查询条件
        whereList.addAll(rangeList);
        return whereList;
    }

    private String parserWhereEntrance(List<Where> whereList) {
        String result = _parserWhereEntrance(null, whereList);
        return result;
    }

    /**
     *
     * @param field 只在NESTED查询时才不为空
     * @param whereList
     * @return
     */
    private String _parserWhereEntrance(String field, List<Where> whereList) {
        if(CollectionUtils.isEmpty(whereList)) {
            return "{\"match_all\":{}}";
        }
        if(whereList.size() > 1) {
            Where where = new Where();
            where.setType(Rel.AND);
            where.setParams(whereList);
            return buildLogicCondition(field, where);
        }
        Where where = whereList.get(0);
        if(where.getType() == Rel.OR || where.getType() == Rel.AND || where.getType() == Rel.NOT) {
            return buildLogicCondition(field, where);
        }
        where = new Where();
        where.setType(Rel.AND);
        where.setParams(whereList);
        return buildLogicCondition(field, where);
    }
}
