package com.meiya.whalex.db.entity.ani;

import com.ejlchina.okhttps.HTTP;
import com.meiya.whalex.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.StatementConfiguration;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * 因为 Doris 可以通过 http 方式交互，因此重新定义这个链接，把 HTTP 连接池对象放进来统一管理
 *
 * @author 黄河森
 * @date 2024/6/28
 * @package com.meiya.whalex.db.entity.ani
 * @project whalex-data-driver
 * @description DorisQueryRunner
 */
@Slf4j
public class DorisQueryRunner extends QueryRunner implements AutoCloseable {

    /**
     * 连接地址模板
     */
    public static String HTTP_URL_TEMPLATE = "/%s/%s/_stream_load";

    Random random = new Random();

    private List<HTTP> httpClients;

    public DorisQueryRunner() {
    }

    public DorisQueryRunner(boolean pmdKnownBroken) {
        super(pmdKnownBroken);
    }

    public DorisQueryRunner(DataSource ds) {
        super(ds);
    }

    public DorisQueryRunner(StatementConfiguration stmtConfig) {
        super(stmtConfig);
    }

    public DorisQueryRunner(DataSource ds, boolean pmdKnownBroken) {
        super(ds, pmdKnownBroken);
    }

    public DorisQueryRunner(DataSource ds, StatementConfiguration stmtConfig) {
        super(ds, stmtConfig);
    }

    public DorisQueryRunner(DataSource ds, boolean pmdKnownBroken, StatementConfiguration stmtConfig) {
        super(ds, pmdKnownBroken, stmtConfig);
    }

    public DorisQueryRunner(List<HTTP> httpClients) {
        this.httpClients = httpClients;
    }

    public DorisQueryRunner(boolean pmdKnownBroken, List<HTTP> httpClients) {
        super(pmdKnownBroken);
        this.httpClients = httpClients;
    }

    public DorisQueryRunner(DataSource ds, List<HTTP> httpClients) {
        super(ds);
        this.httpClients = httpClients;
    }

    public DorisQueryRunner(StatementConfiguration stmtConfig, List<HTTP> httpClients) {
        super(stmtConfig);
        this.httpClients = httpClients;
    }

    public DorisQueryRunner(DataSource ds, boolean pmdKnownBroken, List<HTTP> httpClients) {
        super(ds, pmdKnownBroken);
        this.httpClients = httpClients;
    }

    public DorisQueryRunner(DataSource ds, StatementConfiguration stmtConfig, List<HTTP> httpClients) {
        super(ds, stmtConfig);
        this.httpClients = httpClients;
    }

    public DorisQueryRunner(DataSource ds, boolean pmdKnownBroken, StatementConfiguration stmtConfig, List<HTTP> httpClients) {
        super(ds, pmdKnownBroken, stmtConfig);
        this.httpClients = httpClients;
    }

    /**
     * 通过 HTTP 方式 load 数据
     *
     * @param database
     * @param tableName
     * @param jsonData
     * @param groupCommit
     * @return
     */
    public boolean insertByStreamLoad(String database, String tableName, String jsonData, String groupCommit) throws Exception {

        HTTP httpClient = getHttp();

        Request.Builder builder = new Request.Builder();
        if (StringUtils.isNotBlank(groupCommit)) {
            builder.addHeader("group_commit", groupCommit);
        } else {
            builder.addHeader("label", UUID.randomUUID().toString());
        }
        builder.method("PUT", RequestBody.create(MediaType.parse("text/plain; charset=UTF-8"), jsonData));
        builder.url(httpClient.newBuilder().baseUrl() + String.format(HTTP_URL_TEMPLATE, database, tableName));
        Response response = null;
        try {
            response = httpClient.request(builder.build())
                    .execute();
            if (!response.isSuccessful()) {
                int code = response.code();
                String message = response.message();
                throw new BusinessException("doris stream load data fail! url: " + response.request().url().url().toString() + ", http code: " + code + ", message: " + message);
            }
            ResponseBody body = response.body();
            String loadResult = "";
            if (body != null) {
                loadResult = body.string();
            }
            if (!loadResult.contains("OK")) {
                throw new BusinessException("doris stream load data fail! message: " + loadResult);
            }
            if (log.isDebugEnabled()) {
                log.debug("doris stream load api response body: {}", loadResult);
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
        return true;
    }


    private HTTP getHttp() {
        if(httpClients.size() == 0) {
            throw new BusinessException("集群无可用连接，请检查配置是否正确，服务是否正常");
        }
        int size = httpClients.size();
        int i = random.nextInt(size);
        HTTP http = httpClients.get(i);
        return http;
    }

    @Override
    public void close() throws Exception {
        if (this.httpClients != null) {
            for (HTTP httpClient : httpClients) {
                httpClient.cancelAll();
                httpClient.executor().shutdown();
            }
        }
    }
}
