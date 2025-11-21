package com.meiya.whalex.db.util.helper.impl.ani;

import cn.hutool.crypto.digest.DigestUtil;
import com.ejlchina.okhttps.HTTP;
import com.google.common.net.HttpHeaders;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.db.entity.ani.*;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.encrypt.AESUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import okhttp3.internal.connection.Transmitter;
import okhttp3.internal.http.RealInterceptorChain;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * MySql 配置管理工具
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */

@Slf4j
public class BaseDorisConfigHelper extends BaseMySqlConfigHelper<DorisQueryRunner, DorisDatabaseInfo, DorisTableInfo, RdbmsCursorCache> {

    /**
     * 连接地址模板
     */
    public static String HTTP_URL_TEMPLATE = "http://%s:%s/api";

    @Override
    public DorisTableInfo initTableConfig(TableConf conf) {
        String tableName = conf.getTableName();
        if (StringUtils.isBlank(tableName)) {
            log.error("mysql init table config fail, tableName is null! dbId: [{}]", conf.getId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据表配置必填参数缺失，请校验参数配置是否正确!");
        }
        DorisTableInfo tableInfo;
        String tableJson = conf.getTableJson();
        if (StringUtils.isNotBlank(tableJson)) {
            tableInfo = JsonUtil.jsonStrToObject(tableJson, DorisTableInfo.class);
            String groupCommit = tableInfo.getGroupCommit();
            if (StringUtils.isNotBlank(groupCommit)) {
                if (!StringUtils.equalsAny(groupCommit, DorisTableInfo.GROUP_COMMIT_ASYNC_MODE, DorisTableInfo.GROUP_COMMIT_SYNC_MODE, DorisTableInfo.GROUP_COMMIT_SYNC_MODE)) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "groupCommit 配置值无法识别: " + groupCommit);
                }
            }
        } else {
            tableInfo = new DorisTableInfo();
        }
        tableInfo.setTableName(tableName);
        return tableInfo;
    }

    @Override
    public DorisDatabaseInfo initDbModuleConfig(DatabaseConf conf) {

        List<DorisDatabaseInfo.IpPort> ipPorts = new ArrayList<>();

        String connSetting = conf.getConnSetting();
        Map<String, Object> dbConfMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
        String serviceUrl = (String) dbConfMap.get("serviceUrl");
        String port = dbConfMap.get("port") == null ? null : String.valueOf(dbConfMap.get("port"));
        String fePort = dbConfMap.get("fePort") == null ? null : String.valueOf(dbConfMap.get("fePort"));
        if(StringUtils.isBlank(serviceUrl)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "地址不能为空");
        }
        String[] urls = serviceUrl.split(",");
        String[] ports = null;
        if(StringUtils.isNotBlank(port)) {
            ports = port.split(",");
        }
        String[] fePorts = null;
        if(StringUtils.isNotBlank(fePort)) {
            fePorts = fePort.split(",");
        } else {
          fePorts = new String[urls.length];
            for (int i = 0; i < fePorts.length; i++) {
                fePorts[i] = null;
            }
        }
        for (int i = 0; i < urls.length; i++) {
            String url = urls[i];
            DorisDatabaseInfo.IpPort ipPort = new DorisDatabaseInfo.IpPort();
            if(url.contains(":")) {
                String[] split = serviceUrl.split(":");
                if(split.length != 2) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, serviceUrl + "地址错误");
                }
                ipPort.setIp(split[0].trim());
                ipPort.setPort(split[1].trim());
            }else {
                ipPort.setIp(url.trim());
                ipPort.setPort(ports[i].trim());
            }
            ipPort.setFePort(fePorts[i] == null ? null : fePorts[i].trim());
            ipPorts.add(ipPort);
        }

        String databaseName = (String) dbConfMap.get("dbaseName");
        if (StringUtils.isBlank(databaseName)) {
            databaseName = (String) dbConfMap.get("database");
        }
        String userName = (String) dbConfMap.get("userName");
        if (StringUtils.isBlank(userName)) {
            userName = (String) dbConfMap.get("username");
        }
        String password = (String) dbConfMap.get("password");
        if (StringUtils.isBlank(password)) {
            password = (String) dbConfMap.get("userPassword");
        }
        // AES 解密
        if (StringUtils.isNotBlank(password)) {
            try {
                password = AESUtil.decrypt(password);
            } catch (Exception e) {
//                log.error("mysql init db config password [{}] decrypt fail!!!", password, e);
            }
        }

        String catalog = (String) dbConfMap.get("catalog");

        if (StringUtils.isBlank(userName)
                || StringUtils.isBlank(password)
                || ipPorts.size() == 0) {
            log.error("mysql init database config fail, must field info is null! bigResourceId: [{}]", conf.getBigdataResourceId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
        }
        Integer initialSize = (Integer) dbConfMap.get("initialSize");
        Integer minIdle = (Integer) dbConfMap.get("minIdle");
        Integer maxActive = (Integer) dbConfMap.get("maxActive");
        Integer timeOut = (Integer) dbConfMap.get("timeOut");
        Integer leakDetectionThreshold = (Integer) dbConfMap.get("leakDetectionThreshold");
        DorisDatabaseInfo dorisDatabaseInfo = new DorisDatabaseInfo();
        dorisDatabaseInfo.setIpPorts(ipPorts);
        dorisDatabaseInfo.setUserName(userName);
        dorisDatabaseInfo.setPassword(password);
        dorisDatabaseInfo.setDatabaseName(databaseName);
        dorisDatabaseInfo.setCatalog(catalog);
        if (initialSize != null) {
            dorisDatabaseInfo.setInitialSize(initialSize);
        }
        dorisDatabaseInfo.setMinIdle(minIdle == null ? threadConfig.getCorePoolSize() : minIdle);
        dorisDatabaseInfo.setMaxActive(maxActive == null ? threadConfig.getMaximumPoolSize() : maxActive);
        dorisDatabaseInfo.setTimeOut(timeOut == null ? threadConfig.getTimeOut() * 1000 : timeOut * 1000);
        dorisDatabaseInfo.setLeakDetectionThreshold(leakDetectionThreshold == null ? 0L : leakDetectionThreshold * 1000);

        // 编码方式
        String characterEncoding = (String) dbConfMap.get("characterEncoding");
        if (StringUtils.isNotBlank(characterEncoding)) {
            dorisDatabaseInfo.setCharacterEncoding(characterEncoding);
        }

        // 是否开启 SSL
        Boolean useSSL = dbConfMap.get("useSSL") == null ? null : Boolean.valueOf(String.valueOf(dbConfMap.get("useSSL")));
        if (useSSL != null) {
            dorisDatabaseInfo.setUseSSL(useSSL);
        }

        // 是否开启 tinyInt1isBit
        Boolean tinyInt1isBit = dbConfMap.get("tinyInt1isBit") == null ? null : Boolean.valueOf(String.valueOf(dbConfMap.get("tinyInt1isBit")));
        if (tinyInt1isBit != null) {
            dorisDatabaseInfo.setTinyInt1isBit(tinyInt1isBit);
        }

        // 时区
        Object serverTimezone = dbConfMap.get("serverTimezone");
        if (serverTimezone != null) {
            dorisDatabaseInfo.setServerTimezone(String.valueOf(serverTimezone));
        }

        // 是否开启 tinyInt1isBit
        Boolean bit1isBoolean = dbConfMap.get("bit1isBoolean") == null ? null : Boolean.valueOf(String.valueOf(dbConfMap.get("bit1isBoolean")));
        if (bit1isBoolean != null) {
            dorisDatabaseInfo.setBit1isBoolean(bit1isBoolean);
        } else {
            dorisDatabaseInfo.setBit1isBoolean(true);
        }

        // 是否开启 tinyInt1isBit
        Boolean jsonToObject = dbConfMap.get("jsonToObject") == null ? null : Boolean.valueOf(String.valueOf(dbConfMap.get("jsonToObject")));
        if (jsonToObject != null) {
            dorisDatabaseInfo.setJsonToObject(jsonToObject);
        } else {
            dorisDatabaseInfo.setJsonToObject(false);
        }

        Object httpConnectTimeout = dbConfMap.get("httpConnectTimeout");
        Object httpReadTimeout = dbConfMap.get("httpReadTimeout");
        Object httpMaxIdleConnections =  dbConfMap.get("httpMaxIdleConnections");
        if (httpConnectTimeout != null) {
            dorisDatabaseInfo.setHttpConnectTimeout(Long.valueOf(String.valueOf(httpConnectTimeout)));
        }
        if (httpReadTimeout != null) {
            dorisDatabaseInfo.setHttpReadTimeout(Long.valueOf(String.valueOf(httpReadTimeout)));
        }
        if (httpMaxIdleConnections != null) {
            dorisDatabaseInfo.setHttpMaxIdleConnections(Integer.valueOf(String.valueOf(httpMaxIdleConnections)));
        }
        return dorisDatabaseInfo;
    }

    @Override
    public DorisQueryRunner initDbConnect(DorisDatabaseInfo databaseConf, DorisTableInfo tableConf) {
        List<DorisDatabaseInfo.IpPort> ipPorts = databaseConf.getIpPorts();
        String json = JsonUtil.objectToStr(databaseConf);
        List<HTTP> clients = new ArrayList<>();
        StringBuilder ipPortBuilder = new StringBuilder();
        for (DorisDatabaseInfo.IpPort ipPort : ipPorts) {
            DorisDatabaseInfo copy = JsonUtil.jsonStrToObject(json, DorisDatabaseInfo.class);
            copy.setFePort(ipPort.getFePort());
            copy.setServiceUrl(ipPort.getIp());
            copy.setPort(ipPort.getPort());
            String catalog = databaseConf.getCatalog();
            String databaseName = databaseConf.getDatabaseName();
            if(StringUtils.isNotBlank(catalog) && StringUtils.isNotBlank(databaseName)) {
                copy.setDatabaseName(catalog + "." + databaseName);
            }
            if(ipPortBuilder.length() > 0) {
                ipPortBuilder.append(",");
            }
            ipPortBuilder.append(ipPort.getIp()).append(":").append(ipPort.getPort());
            if(StringUtils.isNotBlank(ipPort.getFePort())) {
                clients.add(buildClient(copy));
            }
        }
        String databaseName = databaseConf.getDatabaseName();
        if(StringUtils.isNotBlank(databaseConf.getCatalog()) && StringUtils.isNotBlank(databaseConf.getDatabaseName())) {
            databaseName = databaseConf.getCatalog() + "." + databaseName;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:mysql:loadbalance://").append(ipPortBuilder.toString());
//            sb.append("jdbc:mysql://").append(copy.getServiceUrl()).append(":").append(copy.getPort());
        if(StringUtils.isNotBlank(databaseName)) {
            sb.append("/").append(databaseName);
        }
        sb.append("?characterEncoding=").append(databaseConf.getCharacterEncoding())
                .append("&useUnicode=true&autoReconnect=true&autoReconnectForPools=true");
        String url = sb.toString();
        if (databaseConf.getUseSSL() != null) {
            url = url + "&useSSL=" + databaseConf.getUseSSL();
        }
        if (StringUtils.isNotBlank(databaseConf.getServerTimezone())) {
            url = url + "&serverTimezone=" + databaseConf.getServerTimezone();
        }
        if (databaseConf.getTinyInt1isBit() != null) {
            url = url + "&tinyInt1isBit=" + databaseConf.getTinyInt1isBit();
        }

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setUsername(databaseConf.getUserName());
        hikariConfig.setPassword(databaseConf.getPassword());
        hikariConfig.setDriverClassName(BaseMySqlDatabaseInfo.DRIVER_CLASS_NAME);
        hikariConfig.setConnectionTestQuery(null);
        // 最小连接数
        hikariConfig.setMinimumIdle(databaseConf.getMinIdle());
        // 最大连接数
        hikariConfig.setMaximumPoolSize(databaseConf.getMaxActive());
        hikariConfig.setLeakDetectionThreshold(databaseConf.getLeakDetectionThreshold());
        // 空闲连接超时时间,默认10分钟 hikariConfig.setIdleTimeout(300000);
        // 连接最大生命周期，默认30分钟 hikariConfig.setMaxLifetime();
        // 连接获取超时，默认30s hikariConfig.setConnectionTimeout();
        // 连接有效性校验
//            hikariConfig.setConnectionTestQuery(BaseMySqlDatabaseInfo.VALIDATION_QUERY);
//            hikariConfig.setValidationTimeout(copy.getTimeOut());
        // 连接有效性超时，默认5S hikariConfig.setValidationTimeout();
        // 设置超时时间
        if (databaseConf.getTimeOut() > 0) {
            hikariConfig.addDataSourceProperty("socketTimeout", String.valueOf(databaseConf.getTimeOut()));
        }
        // 连接池名称
        hikariConfig.setPoolName(DigestUtil.md5Hex(getCacheKey(databaseConf, tableConf)));
        HikariDataSource dataSource = null;
        try {
            dataSource = new HikariDataSource(hikariConfig);
        } catch (Exception e) {
            throw new BusinessException(e, ExceptionCode.CREATE_DB_CONNECT_EXCEPTION);
        }
        DorisQueryRunner dorisQueryRunner = new DorisQueryRunner(dataSource, clients);
        return dorisQueryRunner;
    }

    @Override
    protected DorisQueryRunner returnQueryRunner(DorisDatabaseInfo databaseConf, DorisTableInfo tableConf, DataSource dataSource) {
        return new DorisQueryRunner(dataSource);
    }

    /**
     * 构建HTTP
     *
     * @param databaseConf
     * @return
     */
    private HTTP buildClient(DorisDatabaseInfo databaseConf) {
        HTTP http;
        String baseUrl = String.format(HTTP_URL_TEMPLATE, databaseConf.getServiceUrl(), databaseConf.getFePort());
        long connectTimeout = databaseConf.getHttpConnectTimeout() == null ? 10 : databaseConf.getHttpConnectTimeout();
        int maxIdleConnections = databaseConf.getHttpMaxIdleConnections() == null ? this.threadConfig.getMaximumPoolSize() : databaseConf.getHttpMaxIdleConnections();
        long keepAliveDuration = 60;
        long readTimeout = databaseConf.getHttpReadTimeout() == null ? this.threadConfig.getTimeOut() : databaseConf.getHttpReadTimeout();
        http = HTTP.builder()
                // 客户端配置
                .config(builder -> {
                    // 尝试连接超时时间：10s
                    builder.connectTimeout(connectTimeout, TimeUnit.SECONDS);
                    // 连接池：最大连接数 1000，保持1分钟空闲存活时间
                    builder.connectionPool(new ConnectionPool(maxIdleConnections, keepAliveDuration, TimeUnit.SECONDS));
                    // 读取超时时间
                    builder.readTimeout(readTimeout, TimeUnit.SECONDS);
                    // 如果配置 fe 端口，需要配置重定向，而 OKHTTP 只处理 GET 和 HEAD 的请求，因此关闭 OKHTTP 本身的重定向功能，使用自定义
                    builder.followRedirects(false);
                    builder.followSslRedirects(false);
                    // 添加通过请求头
                    builder.addInterceptor(chain -> {
                        Request request = chain.request();
                        request = request.newBuilder().addHeader(HttpHeaders.EXPECT, "100-continue")
                                .addHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(databaseConf.getUserName(), databaseConf.getPassword()))
                                .addHeader(HttpHeaders.CONTENT_TYPE, "text/plain; charset=UTF-8")
                                .addHeader("column_separator", ",")
                                //容错率为0
                                .addHeader("max_filter_ratio", "0")
                                //将数组展开，然后依次解析其中的每一个 Object 作为一行数据。
                                .addHeader("strip_outer_array", "true")
                                .addHeader("format", "json")
                                .build();
                        return chain.proceed(request);
                    });
                    // 如果配置的是 fe 端口，需要进行重定向
                    builder.addInterceptor(chain -> {
                        Request request = chain.request();
                        Response proceed = chain.proceed(request);
                        RealInterceptorChain realInterceptorChain = (RealInterceptorChain) chain;
                        // 如果配置的是 fe 端口，需要进行重定向
                        if (proceed.isRedirect()) {
                            String location = proceed.headers().get("Location");
                            if (StringUtils.isNotBlank(location)) {
                                Request newRequest = request.newBuilder().url(location).build();
                                Transmitter transmitter = realInterceptorChain.transmitter();
                                // OKHTTP 中需要将 exchange 重置，才可以在进行调用
                                transmitter.exchangeDoneDueToException();
                                proceed = realInterceptorChain.proceed(newRequest);
                            }
                        }
                        return proceed;
                    });
                })
                // 设置地址前缀
                .baseUrl(baseUrl)
                .charset(StandardCharsets.UTF_8)
                .build();
        return http;
    }

    /**
     * 封装认证信息
     *
     * @param username
     * @param password
     * @return
     */
    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    @Override
    public void destroyDbConnect(String cacheKey, DorisQueryRunner connect) throws Exception {
        super.destroyDbConnect(cacheKey, connect);
        connect.close();
    }
}
