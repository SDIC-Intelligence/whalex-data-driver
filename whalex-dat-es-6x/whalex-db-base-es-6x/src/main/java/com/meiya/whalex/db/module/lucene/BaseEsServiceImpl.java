package com.meiya.whalex.db.module.lucene;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.db.constant.DatatypePeriodConstants;
import com.meiya.whalex.db.constant.IsolationLevel;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.QueryCursorMethodResult;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.table.infomaration.TableInformation;
import com.meiya.whalex.db.entity.lucene.*;
import com.meiya.whalex.db.module.AbstractDbModuleBaseService;
import com.meiya.whalex.db.module.DatabaseExecuteStatementLog;
import com.meiya.whalex.db.module.DbTransactionModuleService;
import com.meiya.whalex.db.module.NoTransactionModuleService;
import com.meiya.whalex.db.util.common.DatatypePeriodUtils;
import com.meiya.whalex.db.util.helper.impl.lucene.BaseEsConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.meiya.whalex.interior.db.constant.PeriodCycleEnum;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.WildcardRegularConversion;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Es 服务实现类
 *
 * @author 黄河森
 * @date 2019/12/19
 * @project whale-cloud-platformX
 */
@Slf4j
@Support(value = {
        SupportPower.TEST_CONNECTION,
        SupportPower.CREATE,
        SupportPower.UPDATE,
        SupportPower.DELETE,
        SupportPower.SEARCH,
        SupportPower.SHOW_SCHEMA,
        SupportPower.CREATE_TABLE,
        SupportPower.DROP_TABLE,
        SupportPower.MODIFY_TABLE,
        SupportPower.SHOW_TABLE_LIST
})
public class BaseEsServiceImpl<S extends EsClient,
        Q extends EsHandler,
        D extends EsDatabaseInfo,
        T extends EsTableInfo,
        C extends EsCursorCache> extends AbstractDbModuleBaseService<S, Q, D, T, C> {

    /**
     * 缓存索引的表结构
     */
    protected Cache<String, Map<String, String>> INDEX_MAPPING_CACHE = CacheBuilder.newBuilder().expireAfterWrite(60, TimeUnit.MINUTES).build();


    /**
     * 格式化es查询json
     */
    protected Map<String, String> paramMap = Collections.singletonMap("pretty", "true");

    /**
     * 查询模板
     */
    protected static final String SEARCH_TEMPLATE = "/%s/_search?from=%s&size=%s&timeout=%sms&ignore_unavailable=%s";

    /**
     * 查询模板primary_first:优先在主分片上执行，如果主分片挂了。会在副本上执行
     */
    protected static final String PRIMARY_NODE_SEARCH_TEMPLATE = "/%s/_search?from=%s&size=%s&timeout=%sms&preference=primary_first&ignore_unavailable=%s";

    /**
     * 游标查询模板
     */
    protected static final String SEARCH_CURSOR_TEMPLATE = "/%s/_search?scroll=5m&timeout=%sms&size=%s&ignore_unavailable=%s";

    /**
     * 根据游标ID查询数据
     */
    protected static final String SEARCH_CURSOR_BODY_URL_TEMPLATE = "/_search/scroll?scroll=5m";

    /**
     * 通过游标ID查询
     */
    protected static final String SEARCH_CURSOR_BODY_TEMPLATE = "{\n\"scroll_id\": \"%s\"\n}";

    /**
     * 关闭游标
     */
    protected static final String SEARCH_CURSOR_URL_TEMPLATE = "/_search/scroll";

    /**
     * 关闭游标BODY
     */
    protected static final String CLOSE_CURSOR_BODY_TEMPLATE = "{\n\"scroll_id\": \"%s\"\n}";

    /**
     * 默认不设置Mapping
     */
    protected static final String DEFAULT_SCHEMA_TEMPLATE = "{\n" +
            "  \"settings\": {\n" +
            "    \"index\": {\n" +
            "      \"codec\": \"best_compression\",\n" +
            "      \"refresh_interval\": \"120s\",\n" +
            "      \"number_of_shards\": 2,\n" +
            "      \"translog\": {\n" +
            "        \"retention.size\": \"512m\",\n" +
            "        \"durability\": \"async\",\n" +
            "        \"flush_threshold_size\": \"5GB\"\n" +
            "      },\n" +
            "      \"store.type\": \"niofs\",\n" +
            "      \"number_of_replicas\": 1,\n" +
            "      \"routing.allocation.total_shards_per_node\": \"2\"\n" +
            "    }\n" +
            "  }\n" +
            "}";

    /**
     * 索引 mapping 查询模板
     */
    protected static final String SEARCH_MAPPING_TEMPLATE = "/%s/_mapping";

    protected static final String SEARCH_SETTINGS_TEMPLATE = "/%s/_settings";

    /**
     * 索引 template 查询模板
     */
    protected static final String SEARCH_TEMPLATE_TEMPLATE = "/_template/%s";


    /**
     * 更新模板
     * /index/type/_id/_update
     */
    protected static final String UPDATE_BY_ID_TEMPLATE = "/%s/%s/%s/_update?timeout=%sms";

    /**
     * es 别名模板
     */
    protected static final String ALIASES_TEMPLATE = "{\n" +
            "    \"aliases\": {\n" +
            "        \"index_name\": {\n" +
            "            \"is_write_index\": true\n" +
            "        }\n" +
            "    }\n" +
            "}";

    /**
     * es 滚动索引模板
     */
    private static String esRolloverTemplate = "{\n" +
            "    \"conditions\": {\n" +
            "        \"max_age\": \"7d\",\n" +
            "        \"max_docs\": \"1000\",\n" +
            "        \"max_size\": \"5gb\"\n" +
            "    }\n" +
            "}";

    /**
     * 创建定时滚动索引任务
     */
    protected ScheduledFuture<?> policyScheduledFuture = Executors.newScheduledThreadPool(4).scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
            ConcurrentMap<String, RolloverExecEntity> stringRolloverExecEntityConcurrentMap = policyCache.asMap();
            stringRolloverExecEntityConcurrentMap.entrySet().forEach(e -> {
                // 定时手动执行滚动策略
                String tableName = e.getKey();
                RolloverExecEntity rolloverExecEntity = e.getValue();
                String cacheKey = rolloverExecEntity.getCacheKey();
                Map<String, Object> esRolloverPolicyMap = rolloverExecEntity.getEsRolloverPolicyMap();
                HttpEntity entity = new NStringEntity(JsonUtil.objectToStr(esRolloverPolicyMap), ContentType.APPLICATION_JSON);
                BaseEsConfigHelper helper = (BaseEsConfigHelper) getHelper();
                EsClient dbConnect = (EsClient) helper.getDbConnect(cacheKey);
                try {
                    Response response = dbConnect.getRestClient().performRequest("POST", "/" + tableName + "/_rollover", paramMap, entity);
                    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK
                            && response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
                        log.error("es索引表{}执行滚动策略失败", tableName);
                    } else {
                        log.info("es索引表{}执行滚动策略成功", tableName);
                    }
                } catch (Exception ex) {
                    log.error("es索引表{}执行滚动策略失败", tableName);
                }
            });
        }
    }, 1, 5, TimeUnit.MINUTES);

    /**
     * 缓存操作入库并且具有滚动策略的索引
     */
    protected Cache<String, RolloverExecEntity> policyCache = CacheBuilder.newBuilder().expireAfterWrite(24, TimeUnit.HOURS).build();

    /**
     * 缓存操作入库并且具有滚动策略的索引对象
     */
    protected static class RolloverExecEntity {
        private String cacheKey;

        private Map<String, Object> esRolloverPolicyMap;

        public RolloverExecEntity(String cacheKey, EsTableInfo.EsRolloverPolicy esRolloverPolicy) {
            this.cacheKey = cacheKey;
            Map<String, Object> policy = JsonUtil.jsonStrToMap(esRolloverTemplate);
            Map<String, Object> conditions = (Map<String, Object>) policy.get("conditions");
            if (StringUtils.isNotBlank(esRolloverPolicy.getMaxAge())) {
                conditions.put("max_age", esRolloverPolicy.getMaxAge());
            } else {
                conditions.remove("max_age");
            }
            if (esRolloverPolicy.getMaxDocs() != null && esRolloverPolicy.getMaxDocs() > 0) {
                conditions.put("max_docs", esRolloverPolicy.getMaxDocs());
            } else {
                conditions.remove("max_docs");
            }
            if (StringUtils.isNotBlank(esRolloverPolicy.getMaxSize())) {
                conditions.put("max_size", esRolloverPolicy.getMaxSize());
            } else {
                conditions.remove("max_size");
            }
            this.esRolloverPolicyMap = policy;
        }

        public String getCacheKey() {
            return cacheKey;
        }

        public void setCacheKey(String cacheKey) {
            this.cacheKey = cacheKey;
        }

        public Map<String, Object> getEsRolloverPolicyMap() {
            return esRolloverPolicyMap;
        }

        public void setEsRolloverPolicyMap(Map<String, Object> esRolloverPolicyMap) {
            this.esRolloverPolicyMap = esRolloverPolicyMap;
        }
    }

    protected void recordExecuteStatementLog(String method, String url, String paramJson) {
        StringBuilder builder = new StringBuilder();
        builder.append("es操作: method：[").append(method).append("]")
                .append(", url：[").append(url).append("]");
        if(paramJson != null) {
            builder.append(", 参数：[").append(paramJson).append("]");
        }
        DatabaseExecuteStatementLog.set(builder.toString());
    }

    @Override
    protected QueryMethodResult queryMethod(S esClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        RestClient connect = esClient.getQueryClient();
        BaseEsConfigHelper helper = (BaseEsConfigHelper) getHelper();
        HttpEntity queryHttpEntity = queryEntity.getEsQuery().getQueryHttpEntity();
        boolean isAgg = queryEntity.getEsQuery().isAgg();
        int from = queryEntity.getEsQuery().getFrom();
        int size = queryEntity.getEsQuery().getSize();
        List<String> hiddenFields = queryEntity.getEsQuery().getHiddenFields();
        String queryJson = queryEntity.getEsQuery().getQueryJson();
        int timeOut = dbThreadBaseConfig.getTimeOut() * 1000;
        Response response = null;
        List<Map<String, Object>> resultList = new LinkedList<>();
        long total = 0;

        String indexes = helper.getCompressIndexName(queryEntity.getEsQuery().getStartTime(), queryEntity.getEsQuery().getStopTime(), tableConf);

        if (StringUtils.isBlank(indexes)) {
            throw new BusinessException(ExceptionCode.NOT_FOUND_TABLE_EXCEPTION, tableConf.getTableName());
        }

        // 若查询索引是否存在成功的话一次性查询10个索引，否则一个个索引查询
        // 索引 mapping 查询
        Map<String, String> fieldDataTypeMap = getFieldDataTypeMap(esClient, databaseConf, tableConf);

        // 非聚合查询下，10个索引查询一次
        if (!isAgg) {
            // 查询规则
            Boolean segmentationQuery = tableConf.getSegmentationQuery();

            if (!(DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(tableConf.getPeriodType())
                    || (StringUtils.isNotBlank(queryEntity.getEsQuery().getStartTime()) && StringUtils.isNotBlank(queryEntity.getEsQuery().getStopTime())))) {
                if (!segmentationQuery) {
                    indexes = tableConf.getTableName() + "_*";
                }
            }
            // 记录查询语句
            queryEntity.setQueryStr(transitionQueryStr("GET", indexes, queryEntity.getEsQuery(), tableConf.getIgnoreUnavailable()));


            String esUri = getEsUrl(
                    queryEntity.getEsQuery(),
                    tableConf.getIgnoreUnavailable(),
                    from > 0 ? (from - total) < 0 ? 0 : from - total : 0,
                    size - resultList.size() >= 0 ? size - resultList.size() : 0,
                    timeOut,
                    indexes
            );

            // 隐藏字段查询
            if (CollectionUtil.isNotEmpty(hiddenFields)) {
                StringBuilder sb = new StringBuilder("&docvalue_fields=");
                for (String hiddenField : hiddenFields) {
                    sb.append(hiddenField).append(",");
                }
                sb = sb.deleteCharAt(sb.length() - 1);

                esUri = esUri + sb.toString();
            }
            // 路由值配置
            if (queryEntity.getEsQuery().getRoutingValue() != null) {
                esUri = esUri + "&routing=" + queryEntity.getEsQuery().getRoutingValue();
            }
            // 当前索引查询如果发生错误，记录该异常次数，然后继续查询下一个索引
            long start = 0;
            long end = 0;
            try {
                if (log.isDebugEnabled()) {
                    log.debug("es查询语句:\n GET {} \n {}", esUri, queryJson);
                }
                if (!log.isDebugEnabled()) {
                    response = connect.performRequest("GET", esUri, paramMap, queryHttpEntity);
                } else {
                    start = System.currentTimeMillis();
                    response = connect.performRequest("GET", esUri, paramMap, queryHttpEntity);
                    end = System.currentTimeMillis();
                }
            } catch (ResponseException re) {
                // ES服务端异常响应信息，捕获之后往下传递
                response = re.getResponse();
            } catch (Exception e) {
                throw new BusinessException("es query fail, indexName: [" + esUri + "]", e);
            }
            // 获取响应状态码，判断是否请求成功
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                // 不记录 not found 结果
                if (statusCode != HttpStatus.SC_NOT_FOUND) {
                    String errorMsg = EntityUtils.toString(response.getEntity());
                    throw new BusinessException("es query fail, indexName: [" + esUri + "] statusCode: [" + response.getStatusLine().getStatusCode() + "] msg: [" + errorMsg + "]");
                }
            }
            String resultStr = EntityUtils.toString(response.getEntity());

            // 获取返回 total
            Map<String, Object> resultMap = str2Map(resultStr);

            // 从 json 中解析数据
            resultList.addAll(esResult(resultMap, queryEntity.getEsQuery().getHiddenFields(), fieldDataTypeMap, databaseConf.isOpenParallel()));

            if(log.isDebugEnabled()) {
                Object took = resultMap.get("took");
                log.debug("es查询语句:\n GET {} \n {}", esUri, queryJson);
                long all = end - start;
                log.debug("请求es到响应总耗时为：{}ms， es服务器处理耗时为：{}ms，网络传输耗时为：{}ms",all, took, all - Integer.valueOf(took.toString()));
            }

            //兼容es7之后和es7之前版本返回数据结果不同
            if (((Map) (resultMap.get("hits"))).get("total") instanceof LinkedHashMap) {
                Object valueObject = ((Map) ((Map) (resultMap.get("hits"))).get("total")).get("value");
                if (valueObject != null) {
                    total += Long.valueOf(valueObject.toString());
                }
            } else {
                total += ((Number) ((Map) (resultMap.get("hits"))).get("total")).longValue();
            }
        } else {
            // 聚合查询，拼接所有索引名，并且校验拼接的索引名是否都存在
            if (!(DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(tableConf.getPeriodType())
                    || (StringUtils.isNotBlank(queryEntity.getEsQuery().getStartTime()) && StringUtils.isNotBlank(queryEntity.getEsQuery().getStopTime())))) {
                indexes = tableConf.getTableName() + "_*";
            }

            // 记录查询语句
            queryEntity.setQueryStr(transitionQueryStr("GET", indexes, queryEntity.getEsQuery(), tableConf.getIgnoreUnavailable()));

            String esUri = getEsUrl(
                    queryEntity.getEsQuery(),
                    tableConf.getIgnoreUnavailable(),
                    from,
                    size,
                    timeOut,
                    indexes
            );
            // 路由值配置
            if (queryEntity.getEsQuery().getRoutingValue() != null) {
                esUri = esUri + "&routing=" + queryEntity.getEsQuery().getRoutingValue();
            }
            response = connect.performRequest("GET", esUri, paramMap, queryHttpEntity);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                String responseMsg = EntityUtils.toString(response.getEntity());
                throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, "es 响应状态码: [" + statusCode + "], 响应报文: [" + responseMsg + "]");
            }
            // 转换结果为json
            String resultStr = EntityUtils.toString(response.getEntity());
            // 从 json 中解析数据
            resultList.addAll(aggEsResult(resultStr, from));
            // 获取返回 total
            total = 1;
        }
        return new QueryMethodResult(total, resultList);
    }

    private String getEsUrl(EsHandler.EsQuery esQuery, Boolean ignoreUnavailable, long from, int size, int timeOut, String indexes) {

        String preference = esQuery.getPreference();
        Integer terminateAfter = esQuery.getTerminateAfter();
        boolean primaryNode = esQuery.isPrimaryNode();

        String template = SEARCH_TEMPLATE;

        if(StringUtils.isNotBlank(preference)) {
            template = SEARCH_TEMPLATE + "&preference=" + preference;
        }else if(primaryNode) {
            template = PRIMARY_NODE_SEARCH_TEMPLATE;
        }

        if(terminateAfter != null) {
            template += "&terminate_after=" + terminateAfter;
        }

        return String.format(template, indexes, from, size, timeOut, ignoreUnavailable);
    }

    @Override
    protected QueryMethodResult countMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        queryEntity.getEsQuery().setSize(0);
        queryEntity.getEsQuery().setCount(true);
        QueryMethodResult queryMethodResult = queryMethod(connect, queryEntity, databaseConf, tableConf);
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult testConnectMethod(S esClient, D databaseConf) throws Exception {
        RestClient connect = esClient.getRestClient();
        testConnectRestClient(connect, databaseConf);
        RestClient queryClient = esClient.getQueryClient();
        if (queryClient != null) {
            testConnectRestClient(queryClient, databaseConf);
        }
        return new QueryMethodResult();
    }

    /**
     * es 连接测试
     *
     * @param connect
     * @param databaseConf
     * @throws Exception
     */
    protected void testConnectRestClient(RestClient connect, EsDatabaseInfo databaseConf) throws Exception {
        String endpoint = "_cluster/health";
        if (databaseConf.isPreLoadBalancing()) {
            endpoint = "/" + endpoint;
        }
        Response response = connect.performRequest("GET", endpoint);
        boolean check = HttpStatus.SC_OK == response.getStatusLine().getStatusCode() || HttpStatus.SC_CREATED == response.getStatusLine().getStatusCode();
        if (!check) {
            throw new BusinessException(ExceptionCode.LINK_DATABASE_EXCEPTION
                    , "es statusCode: [" + response.getStatusLine().getStatusCode() + "] msg: [" + EntityUtils.toString(response.getEntity()) + "]");
        }
    }

    @Override
    protected QueryMethodResult showTablesMethod(S esClient, Q queryEntity, D databaseConf) throws Exception {
        RestClient connect = esClient.getRestClient();
        String endpoint = "_cat/indices";
        if (databaseConf.isPreLoadBalancing()) {
            endpoint = "/" + endpoint;
        }
        boolean matchFlag = false;
        // 过滤索引名
        String indexMatch = null;
        if (queryEntity != null && queryEntity.getEsListTable() != null && StringUtils.isNotBlank(queryEntity.getEsListTable().getIndexMatch())) {
            indexMatch = queryEntity.getEsListTable().getIndexMatch();
            if (StringUtils.contains(indexMatch, "?")) {
                indexMatch = StringUtils.replace(indexMatch, "?", "*");
                matchFlag = true;
            }
        }
        if (StringUtils.isNotBlank(indexMatch)) {
            endpoint = endpoint + "/" + indexMatch;
        }
        Response response = connect.performRequest("GET", endpoint);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            String responseMsg = EntityUtils.toString(response.getEntity());
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, "es 响应状态码: [" + statusCode + "], 响应报文: [" + responseMsg + "]");
        }
        String resultStr = EntityUtils.toString(response.getEntity());
        List<Map<String, Object>> result = new ArrayList<>();
        List<Map<String, String>> list = str2List(resultStr);

        Map<String, List<String>> table2aliasMap = table2alias(databaseConf, connect, indexMatch);

        Map<String, Long> index2CountMap = new HashMap<>();

        for (Map<String, String> map : list) {
            Map<String, Object> valueMap = new HashMap<>(2);
            String index = map.get("index");
            String docsCount = map.get("docs.count");
            if (StringUtils.isBlank(docsCount)) {
                docsCount = "0";
            }
            Long count = Long.valueOf(docsCount);
            String health = map.get("health");
            String status = map.get("status");
            String storeSize = map.get("store.size");
            String priStoreSize = map.get("pri.store.size");
            valueMap.put("tableName", index);
            valueMap.put("count", count);
            valueMap.put("health", health);
            valueMap.put("status", status);
            valueMap.put("storeSize", storeSize);
            valueMap.put("priStoreSize", priStoreSize);
            List<String> aliasList = table2aliasMap.get(index);
            if(aliasList == null) {
                aliasList = com.meiya.whalex.util.collection.CollectionUtil.EMPTY_LIST;
            }
            valueMap.put("alias", aliasList);
            index2CountMap.put(index, count);
            result.add(valueMap);
        }

        Set<String> aliasSet = new HashSet<>();

        Map<String, Long> alias2CountMap = new HashMap<>();

        for (String index : table2aliasMap.keySet()) {
            List<String> aliasList = table2aliasMap.get(index);
            aliasSet.addAll(aliasList);

            for (String alias : aliasList) {
                Long aliasCount = alias2CountMap.get(alias);
                if(aliasCount == null) aliasCount = 0L;
                Long count = index2CountMap.get(index);
                if(count == null) count = 0L;
                aliasCount += count;
                alias2CountMap.put(alias, aliasCount);
            }
        }

        for (String alias : aliasSet) {
            Map<String, Object> valueMap = new HashMap<>(2);
            valueMap.put("tableName", alias);
            valueMap.put("count", alias2CountMap.get(alias));
            result.add(valueMap);
        }


        if (matchFlag) {
            result = WildcardRegularConversion.matchFilter(queryEntity.getEsListTable().getIndexMatch(), result);
        }
        return new QueryMethodResult(result.size(), result);
    }

    private Map<String, List<String>> table2alias(D databaseConf, RestClient connect, String indexMatch) throws IOException {
        Request request;
        Response response;
        int statusCode;
        String resultStr;
        List<Map<String, String>> list;// 查询索引别名
        String aliases = "_cat/aliases";
        if (databaseConf.isPreLoadBalancing()) {
            aliases = "/" + aliases;
        }
        // 过滤索引名
        if (StringUtils.isNotBlank(indexMatch)) {
            aliases = aliases + "/" + indexMatch;
        }
        request = new Request("GET", aliases);
        response = connect.performRequest(request);
        statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            String responseMsg = EntityUtils.toString(response.getEntity());
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, "es 响应状态码: [" + statusCode + "], 响应报文: [" + responseMsg + "]");
        }
        resultStr = EntityUtils.toString(response.getEntity());
        list = JsonUtil.jsonStrToObject(resultStr, List.class);

        Map<String, List<String>> table2aliasMap = new HashMap<>();

        if (CollectionUtil.isNotEmpty(list)) {

            for (Map<String, String> map : list) {
                String alias = map.get("alias");
                String index = map.get("index");
                List<String> aliasList = table2aliasMap.get(index);
                if(aliasList == null) {
                    aliasList = new ArrayList<>();
                    table2aliasMap.put(index, aliasList);
                }
                aliasList.add(alias);
            }
        }

        return table2aliasMap;
    }

    @Override
    protected QueryMethodResult getIndexesMethod(EsClient connect, EsDatabaseInfo databaseConf, EsTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "esServiceImpl.getIndexesMethod");
    }

    @Override
    protected QueryMethodResult createTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        if (StringUtils.isBlank(tableConf.getPeriodType())) {
            tableConf.setPeriodType(PeriodCycleEnum.ONLY_ONE.getPeriod());
        }
        List<Map<String, Object>> rows = monitorStatusMethod(connect, databaseConf).getRows();
        Map<String, Object> clusterInfoMap = rows.get(0);
        // 如果数据库配置了分片数量，则用数据库配置的，否则获取当前集群数量
        int numberShard = 0;
        Integer numberOfShards = tableConf.getNumberOfShards();
        if(numberOfShards == null) {
            numberShard = tableConf.getBurstZoneNum() > 0 ? tableConf.getBurstZoneNum() : ((Number) (clusterInfoMap.get("number_of_data_nodes"))).intValue();
        }else {
            numberShard = numberOfShards;
        }

        // 执行建表操作
        // 获取周期内的所有表名
        List<String> tableNameList;
        EsHandler.EsCreateTable createTable = queryEntity.getCreateTable();
        // 判断是否有指定建表区间
        if (createTable.getStartTime() == null || createTable.getStopTime() == null) {
            tableNameList = DatatypePeriodUtils.getPeriodTableNameList(tableConf.getTableName()
                    , tableConf.getPeriodType()
                    , tableConf.getPrePeriodValue()
                    , tableConf.getFuturePeriodValue()
                    , tableConf.getPeriodValue());
        } else {
            tableNameList = DatatypePeriodUtils.getPeriodTableNameList(tableConf.getTableName()
                    , tableConf.getPeriodType()
                    , createTable.getStartTime()
                    , createTable.getStopTime());
        }

        //设置建索引的schema，如果没有传建索引字段配置，则用数据库默认配置的schema，如果数据库也没有默认的schema,那就用DEFAULT_SCHEMA_TEMPLATE
        String schema = null;
        Map<String, Object> schemaTemPlate = queryEntity.getCreateTable().getSchemaTemPlate();
        //tableNameList大于1说明需要分表，分表则先配置索引模板，后再根据索引模板建索引
        if (schemaTemPlate == null) {
            schema = tableConf.getSchema();
            if (schema == null &&
                    (tableNameList.size() > 1 || tableConf.getEsRolloverPolicy() != null)) {
                schemaTemPlate = JsonUtil.jsonStrToMap(DEFAULT_SCHEMA_TEMPLATE);
                // 设置索引匹配模板
                List<String> tempList = new ArrayList<>();
                String tableName = tableConf.getTableName();
                tempList.add(tableName + "_*");
                tempList.add("*." + tableName + "_*");
                tempList.add(tableName);
                tempList.add("*." + tableName);
                schemaTemPlate.put("index_patterns", tempList);
                schema = JsonUtil.objectToStr(schemaTemPlate);
            }else if(tableConf.isCreateTemplate()) {
                schemaTemPlate = JsonUtil.jsonStrToMap(DEFAULT_SCHEMA_TEMPLATE);
                List<String> tempList = new ArrayList<>();
                String tableName = tableConf.getTableName();
                tempList.add(tableName);
                schemaTemPlate.put("index_patterns", tempList);
                schema = JsonUtil.objectToStr(schemaTemPlate);
            }
        } else if (schemaTemPlate != null) {
            if (tableNameList.size() > 1 || tableConf.getEsRolloverPolicy() != null) {
                // 设置索引匹配模板
                List<String> tempList = new ArrayList<>();
                String tableName = tableConf.getTableName();
                tempList.add(tableName + "_*");
                tempList.add("*." + tableName + "_*");
                tempList.add(tableName);
                tempList.add("*." + tableName);
                schemaTemPlate.put("index_patterns", tempList);
            } else if(tableConf.isCreateTemplate()){
                List<String> tempList = new ArrayList<>();
                String tableName = tableConf.getTableName();
                tempList.add(tableName);
                schemaTemPlate.put("index_patterns", tempList);
            } else {
                schemaTemPlate.remove("index_patterns");
                schemaTemPlate.remove("order");
            }
            schema = JsonUtil.objectToStr(schemaTemPlate);
            schema = StringUtils.replace(schema, "${docType}", tableConf.getDocType());
        }
        if (StringUtils.isBlank(schema)) {
            log.warn("elasticsearch [{}] schema is null, not set schema!", tableConf.getTableName());
            schema = DEFAULT_SCHEMA_TEMPLATE;
        }

        int replicasNum = tableConf.getReplicas();
        // 设置 shard 和 replicas 数量
        if (numberShard != -1 || replicasNum != -1) {
            Map<String, Object> map = JsonUtil.jsonStrToObject(schema, Map.class);
            Map indexMap = (Map) ((Map) map.get("settings")).get("index");
            indexMap.put("number_of_shards", numberShard);
            // replicasNum == -1 自动计算
            if (replicasNum == -1) {
                replicasNum = 1;
                if (map.get("mappings") == null || ((Map) map.get("mappings")).get(tableConf.getDocType()) == null || ((Map) (((Map) map.get("mappings")).get(tableConf.getDocType()))).get("enabled") == null) {
                    replicasNum = 1;
                } else {
                    if (((Map) (((Map) map.get("mappings")).get(tableConf.getDocType()))).get("enabled").toString().equals("false")) {
                        replicasNum = 0;
                    }
                }
            }
            indexMap.put("number_of_replicas", replicasNum);


            // 判断是否设置了刷新时间
            Integer refreshInterval = tableConf.getRefreshInterval();
            if (refreshInterval != null) {
                indexMap.put("refresh_interval", refreshInterval + "s");
            }
            Integer totalShardsPerNode = tableConf.getTotalShardsPerNode();
            if(totalShardsPerNode != null) {
                indexMap.put("routing.allocation.total_shards_per_node", totalShardsPerNode);
            }

            schema = JsonUtil.objectToStr(map);
        }

        //分表则只创建索引模板
        List<String> failTable = new ArrayList<>();
        if (tableNameList.size() > 1) {
            try {
                // 1.新建模板
                log.info("elasticsearch template: [{}]", schema);
                createTemplate(tableConf.getTableName(), connect, schema);

                // 2.滚动的话就建索引以及设置别名
                if (tableConf.getEsRolloverPolicy() != null) {
                    createRolloverTable(tableNameList, failTable, databaseConf, tableConf, connect);
                }
            } catch (Exception e) {
                throw new BusinessException("创建索引模板失败！", e);
            }
        } else {
            // 不分表那就只创建一张表
            if(tableConf.isCreateTemplate()) {
                //建模版
                createTemplate(tableConf.getTableName(), connect, schema);
            }else if (tableConf.getEsRolloverPolicy() == null) {
                Map<String, String> tableAndParam = new HashMap<>();
                tableAndParam.put(tableNameList.get(0), schema);
                requestEsCreateTable(failTable, tableAndParam, databaseConf, tableConf, connect);
            } else {
                // 若为滚动索引，则需要建立模板
                Map<String, Object> schemaMap = JsonUtil.jsonStrToMap(schema);
                schemaMap.put("index_patterns", CollectionUtil.newArrayList(tableNameList.get(0) + "-*"));
                schemaMap.put("order", 0);
                // 创建模板
                createTemplate(tableConf.getTableName(), connect, JsonUtil.objectToStr(schemaMap));
                // 创建滚动索引
                createRolloverTable(tableNameList, failTable, databaseConf, tableConf, connect);
            }

        }
        // 如果存在创建失败的表，则抛出异常
        if (CollectionUtil.isNotEmpty(failTable)) {
            throw new BusinessException(ExceptionCode.CREATE_TABLE_EXCEPTION, failTable.toString());
        }
        return new QueryMethodResult(tableNameList.size(), null);
    }

    /**
     * 创建滚动索引
     *
     * @param tableNameList
     * @param failTable
     * @param databaseConf
     * @param tableConf
     * @param connect
     */
    protected void createRolloverTable(List<String> tableNameList, List<String> failTable, D databaseConf, T tableConf, S connect) {
        Map<String, String> tableAndParam = new HashMap<>();
        tableNameList.forEach(name -> {
            Map<String, Object> aliasesTemplate = JsonUtil.jsonStrToMap(ALIASES_TEMPLATE);
            Map<String, Object> aliases = (Map<String, Object>) aliasesTemplate.get("aliases");
            aliases.put(name, aliases.get("index_name"));
            aliases.remove("index_name");
            tableAndParam.put(name + "-000001", JsonUtil.objectToStr(aliasesTemplate));
        });
        requestEsCreateTable(failTable, tableAndParam, databaseConf, tableConf, connect);
    }

    /**
     * 创建模板
     *
     * @param tableName
     * @param connect
     * @param schema
     * @throws Exception
     */
    protected void createTemplate(String tableName, EsClient connect, String schema) throws Exception {
        if (templateExists(tableName, connect)) {
            log.info("elasticsearch template: [{}] has exists!", tableName);
        } else {
            HttpEntity entity = new NStringEntity(schema, ContentType.APPLICATION_JSON);
            Response response = connect.getRestClient().performRequest("PUT", "/_template/" + tableName + "/", paramMap, entity);
            if (response.getStatusLine().getStatusCode() != 200 && response.getStatusLine().getStatusCode() != 201) {
                throw new BusinessException("创建索引模板失败, msg: " + EntityUtils.toString(response.getEntity()));
            }
            log.info("create index template success,template name [{}]", tableName);
        }
    }

    /**
     * 请求创建表
     *
     * @param failTable
     * @param tableAndParam
     * @param databaseConf
     * @param tableConf
     * @param connect
     */
    protected void requestEsCreateTable(List<String> failTable, Map<String, String> tableAndParam, D databaseConf, T tableConf, S connect) {
        tableAndParam.entrySet().forEach(o -> {
            String tableName = o.getKey();
            String param = o.getValue();
            // 判断表是否已经存在
            if (indexExists(tableName, connect)) {
                log.warn("elasticsearch table: [{}] exists!", tableName);
            } else {
                // 执行建表任务
                if (log.isDebugEnabled()) {
                    log.debug("begin elasticsearch create table: [{}]", tableName);
                }
                try {
                    Response response;
                    if (log.isDebugEnabled()) {
                        log.debug("elasticsearch table: [{}]", param);
                    }
                    HttpEntity entity = new NStringEntity(param, ContentType.APPLICATION_JSON);
                    response = connect.getRestClient().performRequest("PUT", tableName, paramMap, entity);
                    if (response.getStatusLine().getStatusCode() != 200 && response.getStatusLine().getStatusCode() != 201) {
                        log.error("elasticsearch create table fail! table: [{}], msg: [{}]", tableName, EntityUtils.toString(response.getEntity()));
                        failTable.add(tableName);
                    }
                } catch (Exception e) {
                    log.error("elasticsearch create table fail! table: [{}]", tableName, e);
                    failTable.add(tableName);
                }
                if (log.isDebugEnabled()) {
                    log.debug("end elasticsearch create table: [{}]", tableName);
                }
            }
        });
    }

    protected boolean templateExists(String tableName, EsClient connect) {
        try {
            Response response = connect.getRestClient().performRequest("GET", "/_template/" + tableName + "/", paramMap);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return true;
            }
        } catch (IOException e) {
            if (((ResponseException) e).getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return false;
            }
        }
        return false;
    }

    @Override
    protected QueryMethodResult dropTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        EsHandler.EsDropTable dropTable = queryEntity.getDropTable();


        String alias = dropTable.getAlias();
        if(StringUtils.isNotBlank(alias)) {
            String removeAliasTemplate = "{\"actions\": [{\"remove\": {\"index\":\"%s\", \"alias\": \"%s\"}}]}";
            removeAliasTemplate = String.format(removeAliasTemplate, tableConf.getTableName(), alias);
            HttpEntity bulkHttpEntity = new NStringEntity(removeAliasTemplate, ContentType.APPLICATION_JSON);
            Map<String, String> params = new HashMap<>();
            params.putAll(paramMap);
            Response postResponse = connect.getRestClient().performRequest("POST", "/_aliases", params, bulkHttpEntity);
            if (postResponse.getStatusLine().getStatusCode() != 200 && postResponse.getStatusLine().getStatusCode() != 201) {
                throw new BusinessException("删除索引别名失败！");
            }
            return new QueryMethodResult(1, null);
        }

        List<String> tableNameList;
        // 判断是否有指定建表区间
        if (!DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(tableConf.getPeriodType())
                && (dropTable.getStartTime() == null || dropTable.getStopTime() == null)) {
            String firstPeriodTableName = DatatypePeriodUtils.getFirstPeriodTableName(tableConf.getTableName()
                    , tableConf.getPeriodType()
                    , tableConf.getPeriodValue());
            List<String> indexNameList = queryIndexName(connect, databaseConf, tableConf.getTableName() + "_*");
            if (CollectionUtil.isEmpty(indexNameList)) {
                log.error("Es drop table: [{}] fail, database: [{}] not found table!", tableConf.getTableName(), databaseConf.getServerAddr());
                throw new BusinessException(ExceptionCode.DROP_TABLE_MISS_EXCEPTION, null);
            } else {
                tableNameList = indexNameList.stream()
                        .filter((tableName) -> {
                            return StringUtils.startsWithIgnoreCase(tableName, tableConf.getTableName())
                                    && DatatypePeriodUtils.getPattern(tableConf.getTableName(), tableConf.getPeriodType(), tableConf.getEsRolloverPolicy() != null).matcher(tableName).find()
                                    // 判断是否滚动索引，若为滚动索引，则去除掉滚动索引后缀 -xxxxx 之后进行比较
                                    && tableConf.getEsRolloverPolicy() != null ? StringUtils.removePattern(tableName, "-\\d+$").compareTo(firstPeriodTableName) < 0 : tableName.compareTo(firstPeriodTableName) < 0;
                        })
                        .collect(Collectors.toList());
                if (CollectionUtil.isEmpty(tableNameList)) {
                    log.error("Es drop table: [{}] fail, database: [{}] not found table!", tableConf.getTableName(), databaseConf.getServerAddr());
                    throw new BusinessException(ExceptionCode.DROP_TABLE_MISS_EXCEPTION, null);
                }
            }
        } else {
            tableNameList = DatatypePeriodUtils.getPeriodTableNameList(tableConf.getTableName()
                    , tableConf.getPeriodType()
                    , dropTable.getStartTime()
                    , dropTable.getStopTime());
            // 若为滚动索引，则需要获取真正索引名
            if (tableConf.getEsRolloverPolicy() != null) {
                tableNameList = tableNameList.stream().flatMap(tableName -> {
                    List<String> indexNames;
                    try {
                        indexNames = queryIndexName(connect, databaseConf, tableName + "-*");
                        // 获取实际索引名之后，在一次进行正则判断是否符合滚动索引名称
                        if (CollectionUtil.isNotEmpty(indexNames)) {
                            Pattern pattern = Pattern.compile(tableName + "-\\d+$");
                            indexNames = indexNames.stream().filter(indexName -> {
                                return pattern.matcher(indexName).find();
                            }).collect(Collectors.toList());
                        }
                    } catch (Exception e) {
                        log.error("current rollover index： [{}] query all indexName fail!", tableName, e);
                        indexNames = new ArrayList<>(0);
                    }
                    return Stream.of(indexNames);
                }).reduce((a, b) -> {
                    if (CollectionUtil.isNotEmpty(a)) {
                        a.addAll(b);
                        return a;
                    } else {
                        return b;
                    }
                }).orElse(null);
                if (CollectionUtil.isEmpty(tableNameList)) {
                    log.error("Es drop rollover index: [{}] fail, database: [{}] not found table!", tableConf.getTableName(), databaseConf.getServerAddr());
                    throw new BusinessException(ExceptionCode.DROP_TABLE_MISS_EXCEPTION, null);
                }
            }
        }
        List<String> failTable = new ArrayList<>();
        // 记录缺失的索引
        AtomicInteger missIndex = new AtomicInteger();
        log.info("elasticsearch drop tables: [{}]", tableNameList);
        if(CollectionUtils.isEmpty(tableNameList)) {
            return new QueryMethodResult(0, null);
        }
        tableNameList.forEach(tableName -> {
            try {
                log.info("elasticsearch drop table: [{}]", tableName);
                if (databaseConf.isPreLoadBalancing()) {
                    tableName = "/" + tableName;
                }
                Response response = connect.getRestClient().performRequest("DELETE", tableName, paramMap);
                if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode() || HttpStatus.SC_CREATED == response.getStatusLine().getStatusCode()) {
                    log.info("elasticsearch drop table success: [{}]", tableName);
                } else {
                    log.error("elasticsearch drop table fail! table: [{}], msg: [{}]", tableName, EntityUtils.toString(response.getEntity()));
                }
            } catch (ResponseException re) {
                // ES服务端异常响应信息，捕获之后往下传递
                int statusCode = re.getResponse().getStatusLine().getStatusCode();
                if (statusCode == HttpStatus.SC_NOT_FOUND) {
                    log.warn("当前执行删除的 tableName: [{}] 不存在!", tableName);
                    missIndex.getAndIncrement();
                } else {
                    log.error("elasticsearch drop table: [{}] fail!", tableName, re);
                    failTable.add(tableName);
                }
            } catch (Exception e) {
                log.error("elasticsearch drop table: [{}] fail!", tableName, e);
                failTable.add(tableName);
            }
        });
        // 判断是否所有执行删除的表均不存在
        if (missIndex.get() == tableNameList.size()) {
            log.error("Es drop table: [{}] fail, database: [{}] not found table!", tableConf.getTableName(), databaseConf.getServerAddr());
            throw new BusinessException(ExceptionCode.DROP_TABLE_MISS_EXCEPTION, null);
        }
        if (CollectionUtil.isNotEmpty(failTable)) {
            throw new BusinessException(ExceptionCode.DROP_TABLE_EXCEPTION, failTable.toString());
        }
        return new QueryMethodResult(tableNameList.size(), null);
    }

    /**
     * 根据索引名规则查询索引
     *
     * @param esClient
     * @param databaseConf
     * @param tableName
     * @return
     * @throws Exception
     */
    protected List<String> queryIndexName(S esClient, D databaseConf, String tableName) throws Exception {
        RestClient connect = esClient.getRestClient();
        String endpoint = "_cat/indices/" + tableName;
        if (databaseConf.isPreLoadBalancing()) {
            endpoint = "/" + endpoint;
        }
        Response response = connect.performRequest("GET", endpoint);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            String responseMsg = EntityUtils.toString(response.getEntity());
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, "es 响应状态码: [" + statusCode + "], 响应报文: [" + responseMsg + "]");
        }
        String resultStr = EntityUtils.toString(response.getEntity());
        List<String> result = new ArrayList<>();
        List<Map<String, String>> list = str2List(resultStr);
        for (Map<String, String> map : list) {
            result.add(map.get("index"));
        }
        return result;
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "esServiceImpl.deleteIndexMethod");
    }

    @Override
    protected QueryMethodResult createIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "esServiceImpl.createIndexMethod");
    }

    @Override
    protected QueryMethodResult insertMethod(S esClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        RestClient connect = esClient.getRestClient();
        EsHandler.EsInsert esInsert = queryEntity.getEsInsert();
        long docSize = esInsert.getDocSize();
        String type = tableConf.getDocType();
        Long captureTime = queryEntity.getEsInsert().getCaptureTime();
        if (!DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(tableConf.getPeriodType()) && captureTime == null) {
            throw new BusinessException(ExceptionCode.DATE_TYPE_TABLE_TIME_NULL_EXCEPTION, "EsServiceImpl.insertMethod");
        }
        Response response;
        String indexName = DatatypePeriodUtils.getPeriodTableName(tableConf.getTableName(), tableConf.getPeriodType(), captureTime);
        if (docSize > 1) {
            String bulkStr = esInsert.getAddStr();
            bulkStr = StringUtils.replaceEach(bulkStr, new String[]{"${indexName}", "${docType}"}, new String[]{indexName, type});
            HttpEntity bulkHttpEntity = new NStringEntity(bulkStr, ContentType.APPLICATION_JSON);
            if (esInsert.isRefresh()) {
                Map<String, String> params = new HashMap<>();
                params.putAll(paramMap);
                params.put("refresh", "true");
                response = connect.performRequest("POST", "/_bulk", params, bulkHttpEntity);
            } else {
                response = connect.performRequest("POST", "/_bulk", paramMap, bulkHttpEntity);
            }
        } else {
            String putStr = esInsert.getAddStr();
            HttpEntity putEntity = new NStringEntity(putStr, ContentType.APPLICATION_JSON);
            String head;
            String id = esInsert.getId();
            Map<String, String> params = new HashMap<>();
            params.putAll(paramMap);
            if (esInsert.getRouteInsert()) {
                params.put("routing", (String) JsonUtil.jsonStrToMap(putStr).get(esInsert.getRoutingField()));
            }
            if (esInsert.isRefresh()) {
                params.put("refresh", "true");
            }
            if (StringUtils.isNotBlank(id)) {
                if (StringUtils.startsWithIgnoreCase(id, "$$$id_")) {
                    if (StringUtils.equalsIgnoreCase(tableConf.getTableName(), "collect")) {
                        head = "/" + indexName + "/" + type + "/" + StringUtils.substringAfter(id, "$$$id_");
                        response = connect.performRequest("PUT", head, params, putEntity);
                    } else {
                        head = "/" + indexName + "/" + type;
                        response = connect.performRequest("POST", head, params, putEntity);
                    }
                } else {
                    head = "/" + indexName + "/" + type + "/" + id;
                    response = connect.performRequest("PUT", head, params, putEntity);
                }
            } else {
                head = "/" + indexName + "/" + type;
                response = connect.performRequest("POST", head, params, putEntity);
            }
        }
        // 如果是滚动索引的话，定时执行滚动策略
        if (tableConf.getEsRolloverPolicy() != null && policyCache.getIfPresent(indexName) == null) {
            BaseEsConfigHelper helper = (BaseEsConfigHelper) getHelper();
            policyCache.put(indexName, new RolloverExecEntity(helper.getCacheKey(databaseConf, tableConf), tableConf.getEsRolloverPolicy()));
        }
        String result = EntityUtils.toString(response.getEntity());
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK
                && response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
            throw new BusinessException("es请求响应错误: " + response.getStatusLine().getStatusCode() + "|" + result);
        }
        Map<String, Object> resultMap = str2Map(result);
        if (MapUtil.isNotEmpty(resultMap)) {
            if (resultMap.get("errors") != null && (Boolean) resultMap.get("errors")) {
                throw new BusinessException("es报文响应错误: " + JsonUtil.objectToStr(resultMap));
            }
        }
        return new QueryMethodResult(queryEntity.getEsInsert().getDocSize(), null);
    }

    @Override
    protected QueryMethodResult updateMethod(S esClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        RestClient connect = esClient.getRestClient();
        BaseEsConfigHelper helper = (BaseEsConfigHelper) getHelper();
        EsHandler.EsUpdate esUpdate = queryEntity.getEsUpdate();
        HttpEntity updateHttpEntity = esUpdate.getUpdateHttpEntity();
        String indexName = helper.getCompressIndexName(esUpdate.getStartTime(), esUpdate.getStopTime(), tableConf);
        if (StringUtils.isBlank(indexName)) {
            throw new BusinessException(ExceptionCode.NOT_FOUND_TABLE_EXCEPTION, tableConf.getTableName());
        }
        StringBuilder indexHead = new StringBuilder("/");
        long timeOut = dbThreadBaseConfig.getTimeOut() * 1000L;
        indexHead.append(indexName)
                .append("/_update_by_query?")
                .append("timeout=")
                .append(timeOut)
                .append("ms")
                .append("&ignore_unavailable=")
                .append(tableConf.getIgnoreUnavailable());
        Map<String, String> params = new HashMap<>();
        params.putAll(paramMap);
        if (esUpdate.isRefresh()) {
            params.put("refresh", "true");
        }
        Response response = connect.performRequest("POST", indexHead.toString(), params, updateHttpEntity);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            String responseMsg = EntityUtils.toString(response.getEntity());
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, "es 响应状态码: [" + statusCode + "], 响应报文: [" + responseMsg + "]");
        }
        // 转换结果为json
        String resultStr = EntityUtils.toString(response.getEntity());
        Map<String, Object> resultMap = str2Map(resultStr);
        long total = Long.valueOf(String.valueOf(resultMap.get("updated")));
        return new QueryMethodResult(total, null);
    }

    @Override
    protected QueryMethodResult delMethod(S esClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        RestClient connect = esClient.getRestClient();
        BaseEsConfigHelper helper = (BaseEsConfigHelper) getHelper();
        EsHandler.EsDel esDel = queryEntity.getEsDel();
        HttpEntity queryHttpEntity = esDel.getQueryHttpEntity();
        String indexName = helper.getCompressIndexName(queryEntity.getEsDel().getStartTime(), queryEntity.getEsDel().getStopTime(), tableConf);
        if (StringUtils.isBlank(indexName)) {
            throw new BusinessException(ExceptionCode.NOT_FOUND_TABLE_EXCEPTION, tableConf.getTableName());
        }


        StringBuilder indexHead = new StringBuilder("/");
        long timeOut = dbThreadBaseConfig.getTimeOut() * 1000L;
        indexHead.append(indexName)
                .append("/_delete_by_query?")
                .append("timeout=")
                .append(timeOut)
                .append("ms")
                .append("&conflicts=proceed&wait_for_completion=false")
                .append("&ignore_unavailable=")
                .append(tableConf.getIgnoreUnavailable());
        Response response = connect.performRequest("POST", indexHead.toString(), paramMap, queryHttpEntity);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            String responseMsg = EntityUtils.toString(response.getEntity());
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, "es 响应状态码: [" + statusCode + "], 响应报文: [" + responseMsg + "]");
        }
        // 转换结果为json
        String resultStr = EntityUtils.toString(response.getEntity());
        Map<String, Object> resultMap = str2Map(resultStr);
//        long total = Long.valueOf(String.valueOf(resultMap.get("total")));
        return new QueryMethodResult(0, Arrays.asList(resultMap));
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(S connect, D databaseConf) throws Exception {
        String endpoint = "_cluster/health";
        if (databaseConf.isPreLoadBalancing()) {
            endpoint = "/" + endpoint;
        }
        Response response = connect.getRestClient().performRequest("GET", endpoint);
        boolean check = HttpStatus.SC_OK == response.getStatusLine().getStatusCode();
        if (!check) {
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION
                    , "es statusCode: [" + response.getStatusLine().getStatusCode() + "] msg: [" + EntityUtils.toString(response.getEntity()) + "]");
        }
        Map<String, Object> map = JsonUtil.jsonStrToObject(EntityUtils.toString(response.getEntity()), Map.class);
        List<Map<String, Object>> resultList = new ArrayList<>(1);
        resultList.add(map);
        return new QueryMethodResult(1, resultList);
    }

    @Override
    protected QueryMethodResult querySchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        RestClient queryClient = connect.getQueryClient();
        BaseEsConfigHelper helper = (BaseEsConfigHelper) getHelper();
        // 查询所有索引名
        List<String> indexNameList = helper.getIndexName(null, null, tableConf);
        if (CollectionUtil.isNotEmpty(indexNameList)) {

            //单表
            if(indexNameList.size() == 1
                    && DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(tableConf.getPeriodType())) {
                String indexName = indexNameList.get(0);
                return querySchemaMethod(connect, tableConf, SEARCH_MAPPING_TEMPLATE, indexName);
            }else {

                //分区表
                EsHandler.EsListTable esListTable = new EsHandler.EsListTable();
                esListTable.setIndexMatch(tableConf.getTableName() + "*");

                EsHandler esHandler = new EsHandler();
                esHandler.setEsListTable(esListTable);

                QueryMethodResult queryMethodResult = showTablesMethod(connect, (Q) esHandler, databaseConf);
                List<Map<String, Object>> rows = queryMethodResult.getRows();
                List<String> tableNameList = rows.stream().map(
                        row -> (String)row.get("tableName")).collect(Collectors.toList());

                for (String indexName : indexNameList) {
                    if(tableNameList.contains(indexName)) {
                        return querySchemaMethod(connect, tableConf, SEARCH_MAPPING_TEMPLATE, indexName);
                    }
                }

                //不存在分区表，查询模版
                return querySchemaMethod(connect, tableConf, SEARCH_TEMPLATE_TEMPLATE, tableConf.getTableName());
            }

        }
        throw new BusinessException(ExceptionCode.NOT_FOUND_TABLE_EXCEPTION, tableConf.getTableName());

    }

    private QueryMethodResult querySchemaMethod(S connect, T tableConf, String path, String indexName) throws Exception {
        RestClient queryClient = connect.getQueryClient();
        String esUri = String.format(path, indexName);
        Response mappingResp = null;
        try {
            mappingResp = queryClient.performRequest("GET", esUri, paramMap);
        } catch (ResponseException e) {
            Response response = e.getResponse();
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_NOT_FOUND) {
                throw new BusinessException(ExceptionCode.NOT_FOUND_TABLE_EXCEPTION, tableConf.getTableName());
            } else {
                throw e;
            }
        }
        // 判断响应状态
        boolean check = HttpStatus.SC_OK == mappingResp.getStatusLine().getStatusCode();
        if (!check) {
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION
                    , "es statusCode: [" + mappingResp.getStatusLine().getStatusCode() + "] msg: [" + EntityUtils.toString(mappingResp.getEntity()) + "]");
        }
        // 解析对象内容
        Map<String, Object> map = JsonUtil.jsonStrToObject(EntityUtils.toString(mappingResp.getEntity()), Map.class);
        // 判断是否滚动索引
        if (tableConf.getEsRolloverPolicy() != null) {
            Map.Entry<String, Object> next = map.entrySet().iterator().next();
            indexName = next.getKey();
        }

        Map<String, Object> indexNameMap = (Map<String, Object>) map.get(indexName);

        if(indexNameMap == null) {
            indexNameMap = (Map<String, Object>) map.values().iterator().next();
        }

        Map<String, Object> indexMappingMap = (Map<String, Object>) indexNameMap.get("mappings");

        Map<String, Object> docMap = (Map<String, Object>) indexMappingMap.get(tableConf.getDocType());
        if (docMap == null) {
            docMap = (Map<String, Object>) indexMappingMap.get("doc");
        }
        if (docMap == null) {
            docMap = (Map<String, Object>) indexMappingMap.get(indexName);
            if (docMap == null) {
                docMap = indexMappingMap;
            }
        }

        // 获取字段内容
        Map<String, Object> propertiesMap = (Map<String, Object>) docMap.get("properties");

        //没有字段时
        if(MapUtil.isEmpty(propertiesMap)) {
            return new QueryMethodResult(0, new ArrayList<>());
        }

        // 结果集
        List<Map<String, Object>> resultList = getPropertiesMaps(connect, tableConf, indexName, indexNameMap, propertiesMap, false);
        return new QueryMethodResult(resultList.size(), resultList);
    }

    /**
     *
     * 解析 properties 获取字段
     *
     * @param connect
     * @param tableConf
     * @param indexName
     * @param indexNameMap
     * @param propertiesMap
     * @return
     */
    private List<Map<String, Object>> getPropertiesMaps(S connect, T tableConf, String indexName, Map<String, Object> indexNameMap, Map<String, Object> propertiesMap, boolean hasAnalysis) {
        // 结果集
        List<Map<String, Object>> resultList = new ArrayList<>(propertiesMap.size());
        Set<String> keySet = propertiesMap.keySet();
        for (String field : keySet) {
            Map<String, Object> fieldMap = (Map<String, Object>) propertiesMap.get(field);
            Map<String, Object> propMap = new HashMap<>();
            propMap.put("col_name", field);
            Set<String> fieldMapKeySet = fieldMap.keySet();
            List<Map<String, Object>> properties = new ArrayList<>();
            for (String key : fieldMapKeySet) {
                Object value = fieldMap.get(key);
                if (StringUtils.equalsIgnoreCase(key, "type")) {
                    propMap.put("data_type", value);
                    ItemFieldTypeEnum itemFieldTypeEnum = EsFieldTypeEnum.dbFieldType2FieldType(String.valueOf(value));
                    propMap.put("std_data_type", itemFieldTypeEnum.getVal());
                } else if (StringUtils.equalsIgnoreCase(key, "null_value")) {
                    propMap.put("columnDef", value);
                } else if (StringUtils.equalsIgnoreCase(key, "ignore_above")) {
                    propMap.put("columnSize", value);
                } else if(!hasAnalysis && StringUtils.equalsIgnoreCase(key, "analyzer")) {
                    //如果是模版
                    Map<String, Object> settings = (Map<String, Object>) indexNameMap.get("settings");
                    if(settings == null) {
                        //mapping 需要调用_settings接口
                        try {
                            settings = getSettings(connect, tableConf, indexName);
                        } catch (IOException e) {
                            log.error("获取索引: [{}]的setting异常", indexName, e);
                        }
                    }
                    if(settings != null) {
                        Map<String, Object> index = (Map<String, Object>) settings.get("index");
                        if(index != null) {
                            Map<String, Object> analysis = (Map<String, Object>) index.get("analysis");
                            if(analysis != null) {
                                propMap.put("analysis", analysis);
                            }
                        }
                    }

                    propMap.put(key, value);
                    hasAnalysis = true;
                } else if (StringUtils.equalsIgnoreCase(key, "properties") && value instanceof Map) {
                    List<Map<String, Object>> nestedPropertiesMap = getPropertiesMaps(connect, tableConf, indexName, indexNameMap, (Map<String, Object>) value, hasAnalysis);
                    properties.addAll(nestedPropertiesMap);
                } else if (StringUtils.equalsIgnoreCase(key, "fields") && value instanceof Map) {
                    // 兼容之前的存放方式
                    propMap.put(key, value);
                    List<Map<String, Object>> fieldsPropertiesMap = getPropertiesMaps(connect, tableConf, indexName, indexNameMap, (Map<String, Object>) value, hasAnalysis);
                    properties.addAll(fieldsPropertiesMap);
                } else {
                    propMap.put(key, value);
                }
            }
            if (CollectionUtil.isNotEmpty(properties)) {
                propMap.put("properties", properties);
            }
            resultList.add(propMap);
        }
        return resultList;
    }

    private Map<String, Object> getSettings(S connect, T tableConf, String indexName) throws IOException {
        RestClient queryClient = connect.getQueryClient();
        String esUri = String.format(SEARCH_SETTINGS_TEMPLATE, indexName);
        recordExecuteStatementLog("GET", esUri, null);
        Response mappingResp = null;
        try {
            mappingResp = queryClient.performRequest("GET", esUri, paramMap);
        } catch (ResponseException e) {
            Response response = e.getResponse();
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_NOT_FOUND) {
                throw new BusinessException(ExceptionCode.NOT_FOUND_TABLE_EXCEPTION, tableConf.getTableName());
            } else {
                throw e;
            }
        }
        // 判断响应状态
        boolean check = HttpStatus.SC_OK == mappingResp.getStatusLine().getStatusCode();
        if (!check) {
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION
                    , "es statusCode: [" + mappingResp.getStatusLine().getStatusCode() + "] msg: [" + EntityUtils.toString(mappingResp.getEntity()) + "]");
        }
        // 解析对象内容
        Map<String, Object> map = JsonUtil.jsonStrToObject(EntityUtils.toString(mappingResp.getEntity()), Map.class);

        Map<String, Object> indexNameMap = (Map<String, Object>) map.get(indexName);

        if(indexNameMap == null) {
            indexNameMap = (Map<String, Object>) map.values().iterator().next();
        }

        return (Map<String, Object>) indexNameMap.get("settings");

    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        return new QueryMethodResult(0L, new ArrayList<>());
    }

    @Override
    protected QueryCursorMethodResult queryCursorMethod(S esClient, Q queryEntity, D databaseConf, T tableConf, C cursorCache, Consumer<Map<String, Object>> consumer, Boolean rollAllData) throws Exception {
        RestClient connect = esClient.getQueryClient();
        BaseEsConfigHelper helper = (BaseEsConfigHelper) getHelper();
        HttpEntity queryHttpEntity = queryEntity.getEsQuery().getQueryHttpEntity();
        Integer batchSize = queryEntity.getEsQuery().getBatchSize();
        List<String> hiddenFields = queryEntity.getEsQuery().getHiddenFields();
        long timeOut = dbThreadBaseConfig.getTimeOut() * 1000L;
        String queryJson = queryEntity.getEsQuery().getQueryJson();
        long total = 0;
        AtomicLong row = new AtomicLong(0);
        Response response = null;
        // 判断是否匹配缓存
        boolean validForCursor = cursorCache != null;
        int actuaIndexStart = 0;
        List<String> indexNameList;
        if (!validForCursor) {
            // 未匹配到游标缓存
            // 查询所有索引名
            indexNameList = helper.getIndexName(queryEntity.getEsQuery().getStartTime(), queryEntity.getEsQuery().getStopTime(), tableConf);
            if (CollectionUtil.isEmpty(indexNameList)) {
                throw new BusinessException(ExceptionCode.NOT_FOUND_TABLE_EXCEPTION, tableConf.getTableName());
            }
        } else {
            // 从缓存从获取批量数
            batchSize = cursorCache.getBatchSize();
            queryEntity.getEsQuery().setBatchSize(cursorCache.getBatchSize());
            // 索引 mapping 查询
            Map<String, String> fieldDataTypeMap = getFieldDataTypeMap(esClient, databaseConf, tableConf);
            // 匹配到缓存
            if (StringUtils.isBlank(cursorCache.getScrollId()) ||
                    cursorById(queryEntity, consumer, connect, batchSize, cursorCache.getCursorIdUrl(), cursorCache.getScrollId(), row, fieldDataTypeMap, databaseConf.isOpenParallel())) {
                // 虽然匹配到缓存,但是并没有有效id了查询所有索引名
                // 或者
                // 缓存中带着理论有效id, 通过id去滚一遍数据, 不够量, 往下继续查询
                // 从下一index开始轮
                indexNameList = cursorCache.getIndexNameList();
                actuaIndexStart = indexNameList.indexOf(cursorCache.getLastIndex()) + 1;
                if (actuaIndexStart == indexNameList.size()) {
                    // 已经结束所有索引
                    return new QueryCursorMethodResult(null);
                }
            } else {
                // 够量, 结束本次查询
                return new QueryCursorMethodResult(cursorCache);
            }
        }

        // 记录查询语句
        queryEntity.setQueryStr(transitionQueryCursorStr("POST", indexNameList, queryEntity.getEsQuery(), tableConf.getIgnoreUnavailable()));
        // 索引 mapping 查询
        Map<String, String> fieldDataTypeMap = getFieldDataTypeMap(esClient, databaseConf, tableConf);
        // 非聚合查询下，逐个索引游标查询
        for (int i = actuaIndexStart; i < indexNameList.size(); i++) {
            String indexName = indexNameList.get(i);
            // 游标查询
            String getCursorIdUrl = String.format(SEARCH_CURSOR_TEMPLATE, indexName, timeOut, batchSize == null ? 100 : batchSize, tableConf.getIgnoreUnavailable());
            // 隐藏字段查询
            if (CollectionUtil.isNotEmpty(hiddenFields)) {
                StringBuilder sb = new StringBuilder("&docvalue_fields=");
                for (String hiddenField : hiddenFields) {
                    sb.append(hiddenField).append(",");
                }
                sb = sb.deleteCharAt(sb.length() - 1);

                getCursorIdUrl = getCursorIdUrl + sb.toString();
            }
            // 当前索引查询如果发生错误，记录该异常次数，然后继续查询下一个索引
            try {
                if (log.isDebugEnabled()) {
                    log.debug("es查询语句:\n POST {} \n {}", getCursorIdUrl, queryJson);
                }
                response = connect.performRequest("POST", getCursorIdUrl, paramMap, queryHttpEntity);
            } catch (ResponseException re) {
                // ES服务端异常响应信息，捕获之后往下传递
                response = re.getResponse();
                log.error("es query statusCode: [{}], indexName: [{}]", response.getStatusLine().getStatusCode(), getCursorIdUrl, re);
            } catch (Exception e) {
                throw new BusinessException("es cursor query fail, indexName: [" + getCursorIdUrl + "]", e);
            }
            // 获取响应状态码，判断是否请求成功
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                // 不记录 not found 结果
                if (statusCode != HttpStatus.SC_NOT_FOUND) {
                    String errorMsg = EntityUtils.toString(response.getEntity());
                    throw new BusinessException("es cursor query fail, indexName: [" + getCursorIdUrl + "] statusCode: [" + response.getStatusLine().getStatusCode() + "] msg: [" + errorMsg + "]");
                } else {
                    continue;
                }
            }

            // 转换结果为json
            String scrollBody = EntityUtils.toString(response.getEntity());
            // 获取返回 scrollId
            Map<String, Object> resultMap = str2Map(scrollBody);
            String scrollId = (String) resultMap.get("_scroll_id");

            if (((Map) (resultMap.get("hits"))).get("total") instanceof LinkedHashMap) {
                Object valueObject = ((Map) ((Map) (resultMap.get("hits"))).get("total")).get("value");
                if (valueObject != null) {
                    total += Long.valueOf(valueObject.toString());
                }
            } else {
                total += ((Number) ((Map) (resultMap.get("hits"))).get("total")).longValue();
            }
            // 从 json 中解析数据
            List<Map<String, Object>> scrollBodyResult = esResult(scrollBody, queryEntity.getEsQuery().getHiddenFields(), fieldDataTypeMap, databaseConf.isOpenParallel());
            row.addAndGet(scrollBodyResult.size());
            if (log.isDebugEnabled()) {
                log.debug("es查询结果:\n size {} \n total{}", scrollBodyResult.size(), ((Number) ((Map) (resultMap.get("hits"))).get("total")).longValue());
            }
            scrollBodyResult.forEach((map) -> consumer.accept(map));
            if (CollectionUtils.isEmpty(scrollBodyResult) || scrollBodyResult.size() < batchSize) {
                // 关闭游标，若游标ID为空，则说明没有任何数据，不产生游标，不需要在额外删除
                if (StringUtils.isNotBlank(scrollId)) {
                    String closeScrollBody = String.format(CLOSE_CURSOR_BODY_TEMPLATE, scrollId);
                    HttpEntity entity = new NStringEntity(closeScrollBody, ContentType.APPLICATION_JSON);
                    connect.performRequest("DELETE", SEARCH_CURSOR_URL_TEMPLATE, paramMap, entity);
                }
                // 当前索引遍历完了，但是还存在其他索引未遍历
                if (row.get() >= batchSize && !rollAllData) {
                    return new QueryCursorMethodResult(total, new EsCursorCache(null, batchSize, indexNameList, indexName, null, getCursorIdUrl));
                }
                continue;
            }
            if (row.get() >= batchSize && !rollAllData) {
                return new QueryCursorMethodResult(total, new EsCursorCache(null, batchSize, indexNameList, indexName, scrollId, getCursorIdUrl));
            }

            // 若 size 为 -1，则单次遍历完全部数据
            if (rollAllData) {
                // 循环拉取数据
                boolean done = true;
                while (done) {
                    done = !cursorById(queryEntity, consumer, connect, batchSize, indexName, scrollId, row, fieldDataTypeMap, databaseConf.isOpenParallel());
                }
            }
        }
        return new QueryCursorMethodResult(total, null);
    }

    /**
     * 根据游标id去滚数据一次
     *
     * @param queryEntity
     * @param consumer
     * @param connect
     * @param batchSize
     * @param getCursorIdUrl
     * @param scrollId
     * @return 游标是否关闭 关闭代表不够量,继续往下index搜索;不关闭代表已经可以,退出本次轮询
     */
    protected boolean cursorById(EsHandler queryEntity, Consumer<Map<String, Object>> consumer, RestClient connect, Integer batchSize, String getCursorIdUrl, String scrollId, AtomicLong row, Map<String, String> fieldDataTypeMap, boolean openParallel) {
        // 游标查询
        Response response = null;
        try {
            String queryScrollBody = String.format(SEARCH_CURSOR_BODY_TEMPLATE, scrollId);
            HttpEntity entity = new NStringEntity(queryScrollBody, ContentType.APPLICATION_JSON);
            response = connect.performRequest("POST", SEARCH_CURSOR_BODY_URL_TEMPLATE, paramMap, entity);
            // 转换结果为json
            String resultStr = EntityUtils.toString(response.getEntity());
            // 从 json 中解析数据
            List<Map<String, Object>> esResult = esResult(resultStr, queryEntity.getEsQuery().getHiddenFields(), fieldDataTypeMap, openParallel);
            esResult.forEach((map) -> consumer.accept(map));
            row.addAndGet(esResult.size());
            if (log.isDebugEnabled()) {
                log.debug("es游标查询语句:\n POST {} \n {}", getCursorIdUrl, scrollId);
                log.debug("es游标查询结果:\n size {}", esResult.size());
            }
            if (CollectionUtils.isEmpty(esResult) || esResult.size() < batchSize) {
                // 关闭游标
                String closeScrollBody = String.format(CLOSE_CURSOR_BODY_TEMPLATE, scrollId);
                HttpEntity closeEntity = new NStringEntity(closeScrollBody, ContentType.APPLICATION_JSON);
                connect.performRequest("DELETE", SEARCH_CURSOR_URL_TEMPLATE, paramMap, closeEntity);
                return true;
            }
        } catch (ResponseException re) {
            // ES服务端异常响应信息，捕获之后往下传递
            response = re.getResponse();
            log.error("es query statusCode: [{}], indexName: [{}]", response.getStatusLine().getStatusCode(), getCursorIdUrl, re);
        } catch (Exception e) {
            log.error("es query fail, indexName: [{}]", getCursorIdUrl, e);
        }
        // 获取响应状态码，判断是否请求成功
        if (response != null && response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            log.error("es query statusCode: [{}], indexName: [{}], response: [{}]", response.getStatusLine().getStatusCode(), getCursorIdUrl, response.toString());
        }
        return false;
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(S esClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        RestClient connect = esClient.getRestClient();
        EsHandler.EsUpsert esUpsert = queryEntity.getEsUpsert();
        String upsertStr = esUpsert.getUpsertStr();
        Long captureTime = esUpsert.getCaptureTime();
        if (!DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(tableConf.getPeriodType()) && captureTime == null) {
            throw new BusinessException(ExceptionCode.DATE_TYPE_TABLE_TIME_NULL_EXCEPTION, "EsServiceImpl.saveOrUpdateMethod");
        }
        String indexName = DatatypePeriodUtils.getPeriodTableName(tableConf.getTableName(), tableConf.getPeriodType(), captureTime);
        long timeOut = dbThreadBaseConfig.getTimeOut() * 1000L;
        String format = String.format(UPDATE_BY_ID_TEMPLATE, indexName, tableConf.getDocType(), esUpsert.getId(), timeOut);
        HttpEntity upsertEntity = new NStringEntity(upsertStr, ContentType.APPLICATION_JSON);
        Map<String, String> params = new HashMap<>();
        params.putAll(paramMap);
        if (esUpsert.isRefresh()) {
            params.put("refresh", "true");
        }
        Response response = connect.performRequest("POST", format, params, upsertEntity);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED) {
            String responseMsg = EntityUtils.toString(response.getEntity());
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, "es 响应状态码: [" + statusCode + "], 响应报文: [" + responseMsg + "]");
        }
        return new QueryMethodResult(1, null);
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(S esClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        RestClient connect = esClient.getRestClient();
        EsHandler.EsUpsertBatch esUpsertBatch = queryEntity.getEsUpsertBatch();
        String bulkStr = esUpsertBatch.getUpsertStr();
        Long captureTime = esUpsertBatch.getCaptureTime();
        if (!DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(tableConf.getPeriodType()) && captureTime == null) {
            throw new BusinessException(ExceptionCode.DATE_TYPE_TABLE_TIME_NULL_EXCEPTION, "EsServiceImpl.saveOrUpdateMethod");
        }
        String indexName = DatatypePeriodUtils.getPeriodTableName(tableConf.getTableName(), tableConf.getPeriodType(), captureTime);
        bulkStr = StringUtils.replaceEach(bulkStr, new String[]{"${indexName}", "${docType}"}, new String[]{indexName, tableConf.getDocType()});
        HttpEntity upsertEntity = new NStringEntity(bulkStr, ContentType.APPLICATION_JSON);
        Map<String, String> params = new HashMap<>();
        params.putAll(paramMap);
        if (esUpsertBatch.isRefresh()) {
            params.put("refresh", "true");
        }
        Response response = connect.performRequest("POST", "/_bulk", params, upsertEntity);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED) {
            String responseMsg = EntityUtils.toString(response.getEntity());
            throw new BusinessException("es 响应状态码: [" + statusCode + "], 响应报文: [" + responseMsg + "]");
        }
        String result = EntityUtils.toString(response.getEntity());
        Map<String, Object> resultMap = str2Map(result);
        if (MapUtil.isNotEmpty(resultMap)) {
            if (resultMap.get("errors") != null && (Boolean) resultMap.get("errors")) {
                throw new BusinessException("es报文响应错误: " + JsonUtil.objectToStr(resultMap));
            }
        }
        return new QueryMethodResult(1, null);
    }

    @Override
    protected QueryMethodResult alterTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        try {
            EsHandler.EsAlterField esAlterField = queryEntity.getEsAlterField();
            Map<String, Object> addFieldMap = esAlterField.getAddFieldMap();
            List<String> deleteFieldList = esAlterField.getDeleteFieldList();
            if(!tableConf.isCreateTemplate() && CollectionUtil.isNotEmpty(addFieldMap)) {
                HashMap<String, Object> properties = new HashMap<>();
                properties.put("properties", addFieldMap);
                String objectToStr = JsonUtil.objectToStr(properties);
                if (log.isDebugEnabled()) {
                    log.debug("es修改mapping语句:{}", objectToStr);
                }
                recordExecuteStatementLog("POST", "/" + tableConf.getTableName() + "/" + tableConf.getDocType() + "/_mapping", objectToStr);
                HttpEntity entity = new NStringEntity(objectToStr, ContentType.APPLICATION_JSON);
                Response putResponse = connect.getRestClient().performRequest("POST", "/" + tableConf.getTableName() + "/" + tableConf.getDocType() + "/_mapping", paramMap, entity);
                if (putResponse.getStatusLine().getStatusCode() != 200 && putResponse.getStatusLine().getStatusCode() != 201) {
                    throw new BusinessException("修改mapping失败！");
                }
            }else if(tableConf.isCreateTemplate() && (CollectionUtil.isNotEmpty(addFieldMap) || CollectionUtil.isNotEmpty(deleteFieldList))) {
                Response response = connect.getRestClient().performRequest("GET", "/_template/" + tableConf.getTableName() + "/", paramMap);
                recordExecuteStatementLog("GET", "/_template/" + tableConf.getTableName() + "/", null);
                if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                    log.info("elasticsearch template: [{}] has not exists!", tableConf.getTableName());
                    throw new BusinessException("索引模板修改失败，模板不存在！");
                }
                Map<String, Object> map = JsonUtil.jsonStrToObject(EntityUtils.toString(response.getEntity()), Map.class);
                Map temp = (Map) map.get(tableConf.getTableName());
                Map mappings = (Map) temp.get("mappings");
                Map<String, Object> docMap = (Map<String, Object>) mappings.get(tableConf.getDocType());
                if (docMap == null) {
                    docMap = (Map<String, Object>) mappings.get("doc");
                }
                //没有字段时，docMap为空，需要新建
                if (docMap == null) {
                    docMap = new HashMap<>();
                    mappings.put(tableConf.getDocType(), docMap);
                }

                Map properties = (Map) docMap.get("properties");
                //没有字段时，properties为空，需要新建
                if (properties == null) {
                    properties = new HashMap();
                    docMap.put("properties", properties);
                }

                // 处理
                if (CollectionUtils.isNotEmpty(deleteFieldList)) {
                    Map finalProperties = properties;
                    deleteFieldList.forEach(element -> {
                        finalProperties.remove(element);
                    });
                }
                if (MapUtils.isNotEmpty(addFieldMap)) {
                    properties.putAll(addFieldMap);
                }

                // 先删除后添加
            /*Response delResponse = connect.getRestClient().performRequest("DELETE", "/_template/" + tableConf.getTableName() + "/", paramMap);
            if (delResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new BusinessException("删除索引模板失败！");
            }*/

                if (log.isDebugEnabled()) {
                    log.debug("es模板创建语句:{}", JsonUtil.objectToStr(temp));
                }
                HttpEntity entity = new NStringEntity(JsonUtil.objectToStr(temp), ContentType.APPLICATION_JSON);
                Response putResponse = connect.getRestClient().performRequest("PUT", "/_template/" + tableConf.getTableName() + "/", paramMap, entity);
                if (putResponse.getStatusLine().getStatusCode() != 200 && putResponse.getStatusLine().getStatusCode() != 201) {
                    throw new BusinessException("创建索引模板失败, msg: " + EntityUtils.toString(putResponse.getEntity()));
                }
                log.info("fix index template success,template name [{}]", tableConf.getTableName());
            }

            String alias = esAlterField.getAlias();
            if(StringUtils.isNotBlank(alias)) {
                String addAliasesTemplate = "{\"actions\": [{\"add\": {\"index\":\"%s\", \"alias\": \"%s\"}}]}";
                addAliasesTemplate = String.format(addAliasesTemplate, tableConf.getTableName(), alias);
                HttpEntity bulkHttpEntity = new NStringEntity(addAliasesTemplate, ContentType.APPLICATION_JSON);
                Map<String, String> params = new HashMap<>();
                params.putAll(paramMap);
                Response postResponse = connect.getRestClient().performRequest("POST", "/_aliases", params, bulkHttpEntity);
                if (postResponse.getStatusLine().getStatusCode() != 200 && postResponse.getStatusLine().getStatusCode() != 201) {
                    throw new BusinessException("创建索引别名失败, msg: " + EntityUtils.toString(postResponse.getEntity()));
                }
                return new QueryMethodResult(1, null);
            }

        } catch (Exception e) {
            throw new BusinessException("修改索引模板失败！", e);
        }
        return new QueryMethodResult(1, null);
    }

    @Override
    protected QueryMethodResult tableExistsMethod(S connect, D databaseConf, T tableConf) throws Exception {
        Response response = connect.getRestClient().performRequest("HEAD", "/" + tableConf.getTableName());
        Map<String, Object> existsMap = new HashMap<>(1);
        if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
            existsMap.put(tableConf.getTableName(), true);
        } else {
            existsMap.put(tableConf.getTableName(), false);
        }
        return new QueryMethodResult(1, CollectionUtil.newArrayList(existsMap));
    }

    @Override
    protected QueryMethodResult queryTableInformationMethod(S connect, D databaseConf, T tableConf) throws Exception {
        Response response = connect.getQueryClient().performRequest("GET", tableConf.getTableName() + "/_settings");
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            String responseMsg = EntityUtils.toString(response.getEntity());
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, "es 响应状态码: [" + statusCode + "], 响应报文: [" + responseMsg + "]");
        }
        String resultStr = EntityUtils.toString(response.getEntity());
        Map<String, Object> settingMap = JsonUtil.jsonStrToMap(resultStr);
        if (settingMap.get("error") != null) {
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, "es 响应状态码: [" + statusCode + "], 响应报文: [" + resultStr + "]");
        }
        Map<String, Object> subMap = (Map<String, Object>) settingMap.get(tableConf.getTableName());
        Map<String, Object> indexMap = (Map<String, Object>) subMap.get("settings");
        Map<String, Object> configMap = (Map<String, Object>) indexMap.get("index");
        Integer shards = Integer.valueOf(String.valueOf(configMap.get("number_of_shards")));
        Integer replicas = Integer.valueOf(String.valueOf(configMap.get("number_of_replicas")));

        // 查询索引信息
        //行
        Map<String, String> indexIndices = getIndexIndices(connect, databaseConf, tableConf);
        // 别名
        List<String> indexAlias = getIndexAlias(connect, databaseConf, tableConf);

        Map<String, Object> extendMeta  = new HashMap<>(indexIndices);
        extendMeta.put("aliases", indexAlias);
        TableInformation tableInformation = TableInformation.builder().replicas(replicas).tableName(tableConf.getTableName()).shards(shards).extendMeta(extendMeta).build();
        return new QueryMethodResult(1, CollectionUtil.newArrayList(JsonUtil.entityToMap(tableInformation)));
    }

    private Map<String, String> getIndexIndices(S connect, D databaseConf, T tableConf) throws IOException {
        String endpoint = "_cat/indices/" + tableConf.getTableName();
        if (databaseConf.isPreLoadBalancing()) {
            endpoint = "/" + endpoint;
        }
        recordExecuteStatementLog("GET", endpoint, null);
        Response response = connect.getQueryClient().performRequest("GET", endpoint);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            String responseMsg = EntityUtils.toString(response.getEntity());
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, "es 响应状态码: [" + statusCode + "], 响应报文: [" + responseMsg + "]");
        }
        String resultStr = EntityUtils.toString(response.getEntity());
        List<Map<String, String>> list = str2List(resultStr);
        Map<String, String> map = list.get(0);
        String docsCount = map.get("docs.count");
        if (StringUtils.isBlank(docsCount)) {
            docsCount = "0";
        }
        map.put("rowCount", docsCount);
        return map;
    }

    /**
     * 获取索引别名
     *
     * @param connect
     * @param tableConf
     * @return
     * @throws IOException
     */
    private List<String> getIndexAlias(S connect, D databaseConf, T tableConf) throws IOException {
        String endpoint = tableConf.getTableName() + "/_alias";
        if (databaseConf.isPreLoadBalancing()) {
            endpoint = "/" + endpoint;
        }
        Request request = new Request("GET", endpoint);
        recordExecuteStatementLog("GET", endpoint, null);
        Response response = connect.getQueryClient().performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            String responseMsg = EntityUtils.toString(response.getEntity());
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, "es 响应状态码: [" + statusCode + "], 响应报文: [" + responseMsg + "]");
        }
        String resultStr = EntityUtils.toString(response.getEntity());
        Map<String, Object> aliasMap = str2Map(resultStr);
        Map<String, Object> tableMap = (Map<String, Object>) aliasMap.get(tableConf.getTableName());
        if (MapUtil.isNotEmpty(tableMap)) {
            Map<String, Object> aliasInfoMap = (Map<String, Object>) tableMap.get("aliases");
            if (MapUtil.isNotEmpty(aliasInfoMap)) {
                Set<String> aliasSet = aliasInfoMap.keySet();
                return new ArrayList<>(aliasSet);
            }
        }
        return com.meiya.whalex.util.collection.CollectionUtil.EMPTY_LIST;
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        return new QueryMethodResult(0L, new ArrayList<>());
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(S connect, Q queryEntity, D databaseConf, T tableConf) {
        return new QueryMethodResult(0L, new ArrayList<>());
    }

    /**
     * 聚合查询结果解析
     *
     * @param resultStr
     * @param offset
     * @return
     * @throws IOException
     */
    protected List<Map<String, Object>> aggEsResult(String resultStr, Integer offset) throws IOException {
        Map<String, Object> aggsMap = new HashMap<>();
        Map<String, Object> resultMap = str2Map(resultStr);
        Map<String, Object> coverResultMap = new HashMap<>();
        // 内存中进行聚合分页
        Map<String, Object> aggregations = (Map<String, Object>) resultMap.get("aggregations");

        if (MapUtil.isNotEmpty(aggregations)) {
            // 处理 topHit 操作下，返回的数据
            aggregations.forEach((k, v) -> {
                transitionHit((Map<String, Object>) v);
            });
        } else {
            aggregations = com.meiya.whalex.util.collection.MapUtil.EMPTY_MAP;
        }
        
        coverResultMap.put("aggregations", aggregations);
        aggsMap.put("aggs", coverResultMap);
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(aggsMap);
        return list;
    }

    /**
     * 转换hit内容
     *
     * @param valueMap
     */
    private void transitionHit(Map<String, Object> valueMap) {
        Map<String, Object> hits = (Map<String, Object>) valueMap.get("hits");
        if (hits != null) {
            hits.remove("max_score");
            List<Map<String, Object>> hitRecords = (List<Map<String, Object>>) hits.get("hits");
            for (int i = 0; i < hitRecords.size(); i++) {
                Map<String, Object> record = hitRecords.get(i);
                Map<String, Object> data = (Map<String, Object>) record.remove("_source");
                record.putAll(data);
            }
        } else if (valueMap.get("buckets") != null) {
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) valueMap.get("buckets");
            parseBucket(buckets);
        }
    }

    /**
     * 解析桶数据
     *
     * @param buckets
     * @return
     */
    private void parseBucket(List<Map<String, Object>> buckets) {
        for (int i = 0; i < buckets.size(); i++) {
            Map<String, Object> bucket = buckets.get(i);
            bucket.forEach((k, v) -> {
                if ((!StringUtils.equalsAnyIgnoreCase(k, "key", "doc_count", "key_as_string")) && v instanceof Map) {
                    transitionHit((Map<String, Object>) v);
                }
            });
        }
    }

    /**
     * 普通查询结果解析
     *
     * @param str
     * @param hiddenFields
     * @return
     */
    protected List<Map<String, Object>> esResult(String str, List<String> hiddenFields, Map<String, String> fieldDataTypeMap, boolean openParallel){
        Map<String, Object> map = str2Map(str);
        return esResult(map, hiddenFields, fieldDataTypeMap, openParallel);
    }

    /**
     * 普通查询结果解析
     *
     * @param map
     * @param hiddenFields
     * @return
     */
    protected List<Map<String, Object>> esResult(Map<String, Object> map, List<String> hiddenFields, Map<String, String> fieldDataTypeMap, boolean openParallel) {
        List<Map<String, Object>> resultList = new ArrayList<>();

        // 推荐语法解析
        Map<String, Object> suggestMap =  (Map<String, Object>)map.get("suggest");
        if (MapUtil.isNotEmpty(suggestMap)) {
            resultList.add(suggestMap);
            return resultList;
        }

        // 解析普通查询
        Map<String, Object> hitsMap = (Map<String, Object>) map.get("hits");
        Object objectMap = hitsMap.get("hits");
        if ("[]".equals(objectMap.toString())) {
            return resultList;
        }

        List<Map<String, Object>> listMap = (List<Map<String, Object>>) objectMap;
        if(openParallel) {
            listMap.parallelStream().forEachOrdered(entryMap-> {
                handleResult(hiddenFields, fieldDataTypeMap, resultList, entryMap);
            });
        }else {
            for (Map<String, Object> entryMap : listMap) {
                handleResult(hiddenFields, fieldDataTypeMap, resultList, entryMap);
            }
        }

        return resultList;
    }

    private void handleResult(List<String> hiddenFields, Map<String, String> fieldDataTypeMap, List<Map<String, Object>> resultList, Map<String, Object> entryMap) {
        Map<String, Object> result_map = new HashMap<>();
        Object id_value = entryMap.get("_id");
        Object indexName = entryMap.get("_index");
        Double score = (Double) entryMap.get("_score");
        result_map.put("_id", id_value);
        result_map.put("_score", score);
        result_map.put("_index", indexName);
        Map<String, Object> sourceMap = (Map<String, Object>) entryMap.get("_source");
        Map<String, Object> docValueFields = (Map<String, Object>) entryMap.get("fields");

        // 若存在查询隐藏字段则做特殊处理
        if (CollectionUtil.isNotEmpty(hiddenFields) && docValueFields != null) {
            if (hiddenFields.size() == docValueFields.size()) {
                sourceMap.putAll(docValueFields);
                docValueFields = null;
            } else {
                Iterator<Map.Entry<String, Object>> iterator = docValueFields.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Object> next = iterator.next();
                    if (hiddenFields.contains(next.getKey())) {
                        sourceMap.put(next.getKey(), next.getValue());
                        iterator.remove();
                    }
                }
                if (docValueFields.size() == 0) {
                    docValueFields = null;
                }
            }
        }
        if (sourceMap != null) {
            sourceMap.entrySet().forEach(entry -> {transformDataType(entry, fieldDataTypeMap);});
        }
        if (MapUtil.isNotEmpty(sourceMap)) {
            result_map.putAll(sourceMap);
        }

        Map<String, Object> highlightMap = (Map<String, Object>) entryMap.get("highlight");
        //高亮操作解析
        if (highlightMap != null) {

            highlightMap.entrySet().parallelStream().forEach(entry -> {
                Object entryValue = entry.getValue();
                if(entryValue instanceof List)  {
                    entry.setValue(entryValue);
                }
            });

            result_map.put("highlight", highlightMap);
        }

        // 折叠操作解析
        Object _fields = docValueFields;
        Object innerHits = entryMap.get("inner_hits");
        if (_fields != null) {
            Map<String, Object> collapseMap = new HashMap<>(2);
            collapseMap.put("fields", _fields);
            if (innerHits != null) {
                collapseMap.put("inner_hits", innerHits);
            }
            result_map.put("collapse", collapseMap);
        }
        resultList.add(result_map);
    }

    private void transformDataType(Map.Entry<String, Object> entry, Map<String, String> fieldDataTypeMap) {

        Object value = entry.getValue();

        if (value == null) {
            return;
        }

        if (value instanceof String) {
            value = formatUTCDateValue((String) value);
            /**
             * 经纬度 从（纬度， 经度）转成（经度，纬度）
             */
            String dataType = fieldDataTypeMap.get(entry.getKey());
            if ("geo_point".equalsIgnoreCase(dataType)) {
                String[] split = ((String) value).split(",");
                if (split.length == 2) {
                    value = split[1] + "," + split[0];
                }
            }
            entry.setValue(value);
            return;
        }


        if(value instanceof Number) {
            return;
        }

        if (value instanceof List) {
            List<Object> valueList = (List<Object>) value;
            if (CollectionUtils.isNotEmpty(valueList)) {
                for (int i = 0; i < valueList.size(); i++) {
                    Object object = valueList.get(i);
                    if (object instanceof Map) {
                        Map<String, Object> valueMap = (Map<String, Object>) object;
                        for (Map.Entry<String, Object> e : valueMap.entrySet()) {
                            Object eValue = e.getValue();
                            if (eValue instanceof String) {
                                value = formatUTCDateValue((String) eValue);
                                e.setValue(eValue);
                            }
                        }
                    } else {
                        Object eValue = object;
                        if (eValue instanceof String) {
                            value = formatUTCDateValue((String) eValue);
                            valueList.set(i, eValue);
                        }
                    }
                }
            }
        }

        if (value instanceof Map) {
            Map<String, Object> valueMap = (Map<String, Object>) value;
            /**
             * 经纬度 从（纬度， 经度）转成（经度，纬度）
             */
            String dataType = fieldDataTypeMap.get(entry.getKey());
            if ("geo_point".equalsIgnoreCase(dataType)) {
                Object lon = valueMap.get("lon");
                Object lat = valueMap.get("lat");
                value = lon + "," + lat;
                entry.setValue(value);
            } else {
                for (Map.Entry<String, Object> e : valueMap.entrySet()) {
                    Object eValue = e.getValue();
                    if (eValue instanceof String) {
                        String newValue = formatUTCDateValue((String) eValue);
                        if (!StringUtils.equalsIgnoreCase(newValue, (String) eValue)) {
                            e.setValue(eValue);
                        }
                    }
                }
            }

        }

    }


    /**
     * esStr To Map
     *
     * @param jsonStr
     * @return
     */
    protected Map<String, Object> str2Map(String jsonStr) {
        Map<String, Object> map = null;
        try {
            map = JsonUtil.jsonStrToMap(jsonStr);
        } catch (Exception e) {
            log.error(e.getMessage() + "转换出错", e);
        }
        return map;
    }

    /**
     * esStr to List
     *
     * @param jsonStr
     * @return
     */
    protected List<Map<String, String>> str2List(String jsonStr) {
        List<Map<String, String>> list = null;
        try {
            list = JsonUtil.jsonStrToObject(jsonStr, List.class);
        } catch (Exception e) {
            log.error(e.getMessage() + "转换出错", e);
        }
        return list;
    }

    /**
     * 格式化UTC时间
     *
     * @param value
     * @return
     */
    protected String formatUTCDateValue(String value) {
        if (StringUtils.isBlank(value)) {
            return value;
        }

        if (!value.contains("T")) {
            return value;
        }

        try {
            if (value.contains("Z")) {
                DateTime dateTime = DateUtil.parseUTC(value);
                value = dateTime.toString();
            } else if (value.contains("T")){
                DateTime dateTime = DateUtil.parse(value, "yyyy-MM-dd'T'HH:mm:ss");
                value = dateTime.toString();
            } else {
                DateTime dateTime = DateUtil.parse(value, "yyyy-MM-dd'T'HH:mm:ss");
                value = dateTime.toString();
            }
        } catch (Exception exception) {
        }
        return value;
    }

    /**
     * 校验索引是否存在
     *
     * @param index
     * @return
     */
    protected boolean indexExists(final String index, final S connect) {
        RestClient restClient = connect.getRestClient();
        // 查询索引名称
        String endpoint = "_cat/indices/" + index + "?h=index,status";
        // 查询索引别名
        String aliases = "_cat/aliases/" + index + "*?h=alias";
        if (connect.isPreLoadBalancing()) {
            endpoint = "/" + endpoint;
            aliases = "/" + aliases;
        }
        Request request;
        Response response;
        try {
            // 判断索引是否存在
            request = new Request("GET", endpoint);
            response = restClient.performRequest(request);
            int statusCode = response.getStatusLine().getStatusCode();
            log.info(endpoint + "查询结果：" + statusCode);
            if (statusCode == HttpStatus.SC_OK) {
                return true;
            }
        } catch (ResponseException responseException) {
            response = responseException.getResponse();
            int statusCode = response.getStatusLine().getStatusCode();
            log.error(endpoint + "查询结果：" + statusCode);
//            if (statusCode != HttpStatus.SC_NOT_FOUND) {
//                return true;
//            }
        } catch (Exception e) {
            log.error("create table operation check index exists fail!", e);
            return true;
        }
        // 判断别名是否存在
        try {
            request = new Request("GET", aliases);
            response = restClient.performRequest(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                String resultStr = EntityUtils.toString(response.getEntity());
                if(StringUtils.isBlank(resultStr)) {
                    resultStr = "[]";
                }
                log.info(aliases + "查询结果：" + resultStr);
                List<Map<String, String>> list = JsonUtil.jsonStrToObject(resultStr, List.class);
                if (CollectionUtil.isNotEmpty(list)) {
                    List<String> alias = list.stream().flatMap(map -> Stream.of(map.get("alias"))).collect(Collectors.toList());
                    return alias.contains(index);
                }
            }
        } catch (ResponseException responseException) {
            response = responseException.getResponse();
            int statusCode = response.getStatusLine().getStatusCode();
            log.error(aliases + "查询结果：" + statusCode);
//            if (statusCode == HttpStatus.SC_NOT_FOUND) {
//                return false;
//            }
        } catch (Exception e) {
            log.error("create table operation check alias exists fail!", e);
            return true;
        }
        return false;
    }

    /**
     * 查询语句（用于日志输出）
     *
     * @param operation
     * @param indexHead
     * @param esQuery
     * @return
     */
    protected String transitionQueryStr(String operation,String indexHead, EsHandler.EsQuery esQuery, Boolean ignoreUnavailable) {
        try {
            String headStr = getEsUrl(
                    esQuery,
                    ignoreUnavailable,
                    esQuery.getFrom(),
                    esQuery.getSize(),
                    (int) (dbThreadBaseConfig.getTimeOut() * 1000L),
                    indexHead
            );
            // 路由值配置
            if (esQuery.getRoutingValue() != null) {
                headStr = headStr + "&routing=" + esQuery.getRoutingValue();
            }
            StringBuilder querySb = new StringBuilder();
            querySb.append(operation)
                    .append(" ")
                    .append(headStr)
                    .append("\n")
                    .append(esQuery.getQueryJson());
            return querySb.toString();
        } catch (Exception e) {
            log.error("elasticsearch query string transition fail!", e);
            return null;
        }
    }

    /**
     * 查询语句（用于日志输出）
     *
     * @param operation
     * @param indexNameList
     * @param esQuery
     * @return
     */
    protected String transitionQueryCursorStr(String operation, List<String> indexNameList, EsHandler.EsQuery esQuery, Boolean ignoreUnavailable) {
        try {
            StringBuilder indexHead = new StringBuilder();
            for (int i = 0; i < indexNameList.size(); i++) {
                String indexName = indexNameList.get(i);
                indexHead.append(indexName).append(",");
            }
            if (indexHead.length() > 0) {
                indexHead.deleteCharAt(indexHead.length() - 1);
            } else {
                indexHead.append("not_found_index");
            }
            Integer batchSize = esQuery.getBatchSize();
            String headStr = String.format(SEARCH_CURSOR_TEMPLATE, indexHead.toString(), dbThreadBaseConfig.getTimeOut() * 1000L, batchSize == null ? 100 : batchSize, ignoreUnavailable);
            StringBuilder querySb = new StringBuilder();
            querySb.append(operation)
                    .append(" ")
                    .append(headStr)
                    .append("\n")
                    .append(esQuery.getQueryJson());
            return querySb.toString();
        } catch (Exception e) {
            log.error("elasticsearch query string transition fail!", e);
            return null;
        }
    }

    /**
     * 获取字段类型 map
     * 使用本地缓存减少查询的次数
     *
     * @param connect
     * @param databaseConf
     * @param tableConf
     * @return
     */
    protected Map<String, String> getFieldDataTypeMap(S connect, D databaseConf, T tableConf) {
        String cacheKey = databaseConf.getServerAddr() + "_" + tableConf.getTableName();
        try {
            Map<String, String> cacheMap = INDEX_MAPPING_CACHE.getIfPresent(cacheKey);
            if (cacheMap == null) {
                QueryMethodResult queryMethodResult = querySchemaMethod(connect, databaseConf, tableConf);
                List<Map<String, Object>> rows = queryMethodResult.getRows();
                Map<String, String> fieldDataTypeMap = new HashMap<>(rows.size());
                rows.forEach(row -> {
                    String field = row.get("col_name").toString();
                    Object data_type = row.get("data_type");
                    String type;
                    if (data_type == null) {
                        type = "Object";
                    } else {
                        type = data_type.toString();
                    }
                    //只缓存是geo_point类型的字段
                    if ("geo_point".equalsIgnoreCase(type)) {
                        fieldDataTypeMap.put(field, type);
                    }
                });
                INDEX_MAPPING_CACHE.put(cacheKey, fieldDataTypeMap);
                return fieldDataTypeMap;
            }
            return cacheMap;
        } catch (Exception e) {
            log.error("查询es索引Schema异常", e);
            INDEX_MAPPING_CACHE.put(cacheKey, MapUtils.EMPTY_MAP);
            return MapUtils.EMPTY_MAP;
        }

    }

    @Override
    protected DbTransactionModuleService getDbTransactionModuleService(DatabaseSetting databaseSetting,
                                                                       String transactionIdKey,
                                                                       String transactionFirstKey,
                                                                       String isolationLevelKey,
                                                                       String transactionId,
                                                                       IsolationLevel isolationLevel) {
        return new NoTransactionModuleService(
                databaseSetting, this, transactionId, isolationLevel, transactionIdKey, transactionFirstKey, isolationLevelKey
        );
    }
}
