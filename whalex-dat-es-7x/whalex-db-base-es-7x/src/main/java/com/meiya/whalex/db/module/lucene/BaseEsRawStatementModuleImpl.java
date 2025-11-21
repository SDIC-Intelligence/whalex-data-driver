package com.meiya.whalex.db.module.lucene;

import com.meiya.whalex.db.entity.DataResult;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.lucene.EsClient;
import com.meiya.whalex.db.entity.lucene.EsCursorCache;
import com.meiya.whalex.db.entity.lucene.EsDatabaseInfo;
import com.meiya.whalex.db.entity.lucene.EsHandler;
import com.meiya.whalex.db.entity.lucene.EsRequestParam;
import com.meiya.whalex.db.entity.lucene.EsTableInfo;
import com.meiya.whalex.db.module.AbstractDbRawStatementModule;
import com.meiya.whalex.db.module.DatabaseExecuteStatementLog;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;


@Slf4j
public abstract class BaseEsRawStatementModuleImpl<S extends EsClient,
        Q extends EsHandler,
        D extends EsDatabaseInfo,
        T extends EsTableInfo,
        C extends EsCursorCache> extends AbstractDbRawStatementModule<S, Q, D, T, C> {


    /**
     * 格式化es查询json
     */
    protected Map<String, String> paramMap = Collections.singletonMap("pretty", "true");

    /**
     * @param body       请求体
     * @param request
     * @param restClient
     * @return
     * @throws IOException
     */
    protected Response executeCall(String body, Request request, RestClient restClient) throws IOException {
        return executeCall(body, request, paramMap, restClient);
    }

    protected Response executeCall(String body, Request request, Map<String, String> paramMap, RestClient restClient) throws IOException {

        NStringEntity entity = null;
        try {
            entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
            request.addParameters(paramMap);
            request.setEntity(entity);
            return restClient.performRequest(request);
        } finally {
            if (entity != null) entity.close();
        }
    }

    private EsRequestParam rawStatementParse(String rawStatement) {
        if(StringUtils.isBlank(rawStatement)) {
            throw new RuntimeException("请求语句不能为空");
        }

        String[] split = rawStatement.split("\n");
        String urlAndMethodStr = split[0];
        String[] urlAndMethod = urlAndMethodStr.split("\\s+");
        if(urlAndMethod.length != 2) {
            throw new RuntimeException("无效的请求路径：" + urlAndMethodStr);
        }
        String method = urlAndMethod[0];
        String url = urlAndMethod[1];
        StringBuilder paramBuilder = new StringBuilder();
        if(split.length > 1) {
            for (int i = 1; i < split.length; i++) {
                paramBuilder.append(split[i]);
                paramBuilder.append("\n");
            }
        }
        return new EsRequestParam(url, method.toUpperCase(), paramBuilder.toString());
    }


    @Override
    public DataResult rawStatementExecute(DatabaseSetting databaseSetting, String rawStatement) throws Exception {

        DataResult dataResult = _execute(databaseSetting, (D dataConf) -> rawStatementParse(rawStatement), this::execute);
        return dataResult;
    }

    public <R, P> R execute(D dataConf, S dbConnect, P params) {
        RestClient restClient = dbConnect.getRestClient();
        EsRequestParam esRequestParam = (EsRequestParam) params;
        String method = esRequestParam.getMethod();
        String url = esRequestParam.getUrl();
        String paramsStr = esRequestParam.getParams();

        Request request = new Request(method, url);
        recordExecuteStatementLog(method, url, paramsStr);
        Response response = null;
        try {
            response = executeCall(paramsStr, request, restClient);
        } catch (ResponseException re) {
            // ES服务端异常响应信息，捕获之后往下传递
            response = re.getResponse();
        } catch (Exception e) {
            throw new BusinessException("执行语句失败：" + url, e);
        }

        DataResult dataResult = new DataResult();
        // 获取响应状态码，判断是否请求成功
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED) {
            dataResult.setCode(ReturnCodeEnum.CODE_SYSTEM_ERROR.getCode());
            if (statusCode != HttpStatus.SC_NOT_FOUND) {
                String errorMsg = null;
                try {
                    errorMsg = EntityUtils.toString(response.getEntity());
                } catch (IOException e) {
                    errorMsg = e.getMessage();
                    e.printStackTrace();
                }
                dataResult.setMessage(errorMsg);
                dataResult.setSuccess(false);
            }
        }
        String resultStr = null;
        try {
            resultStr = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
            resultStr = e.getMessage();
        }
        if(resultStr.startsWith("[")) {
            List<Map<String, String>> maps = str2List(resultStr);
            dataResult.setData(maps);
        }else if(resultStr.startsWith("{")){
            Map<String, Object> map = str2Map(resultStr);
            dataResult.setData(map);
        }else {
            dataResult.setData(resultStr);
        }
        return (R) dataResult;
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


    protected void recordExecuteStatementLog(String method, String url, String paramJson) {
        StringBuilder builder = new StringBuilder();
        builder.append("es操作: method：[").append(method).append("]")
                .append(", url：[").append(url).append("]");
        if(paramJson != null) {
            builder.append(", 参数：[").append(paramJson).append("]");
        }
        DatabaseExecuteStatementLog.set(builder.toString());
    }

}
