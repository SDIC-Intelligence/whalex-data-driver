package com.meiya.whalex.jdbc;

import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.jdbc.factory.DatabaseConfFactory;
import com.meiya.whalex.starter.InitDbModuleStartupRunner;
import com.meiya.whalex.util.DbBeanManagerUtil;
import com.meiya.whalex.util.GetBeanNameUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * dat驱动对象
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
@Slf4j
public class DatDriver implements java.sql.Driver{

    private static final Pattern SCHEME_PTRN = Pattern.compile("(?<scheme>[\\w:%]+).*");
    private static final Pattern URL_PTRN = Pattern.compile("jdbc:(?<dbType>.+):dat://(?<url>.+)");
    private static final Pattern URL_PTRN2 = Pattern.compile("jdbc:dat:(?<dbType>.+)://(?<url>.+)");
    private static final String SCHEME = "jdbc:";
    private static final String SCHEME2 = "jdbc:dat:";
    private static boolean classIsLoad = false;
    private static Map<String, String > dbType2AliasMap = new HashMap<>();

    static {
        dbType2AliasMap.put("postgresql", DbResourceEnum.postgre.name());
        dbType2AliasMap.put("kingbase8", DbResourceEnum.kingbasees.name());
    }
    
    public DatDriver() throws SQLException {
    }

    static {
        try {
            if(!classIsLoad) {
                classIsLoad = true;
                // 全局只调用一次！！！
                DatabaseConfFactory.init();
                InitDbModuleStartupRunner.init();
                DriverManager.registerDriver(new DatDriver());
            }

        } catch (Exception var1) {
            throw new RuntimeException("Can't register driver!");
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {

        //验证url有效性
        if(!acceptsURL(url)){
            return null;
        }

        Map<String, Object> params = new HashMap<>();

        String connectionStr = url;

        //解析url
        String[] split = url.split("\\?");
        if(split.length == 2) {

            connectionStr = split[0];

            String[] paramStrArray = split[1].split("&");
            for (String paramStr : paramStrArray) {
                String[] keyValue = paramStr.split("=");
                params.put(keyValue[0], keyValue[1]);
            }
        }

        Matcher matcher = null;

        if(connectionStr.startsWith(SCHEME2)) {
            matcher = URL_PTRN2.matcher(connectionStr);
        }else {
            matcher = URL_PTRN.matcher(connectionStr);
        }

        matcher.matches();
        String dbType = matcher.group("dbType");
        String alias = dbType2AliasMap.get(dbType);
        if(alias != null) {
            dbType = alias;
        }
        String urlStr = matcher.group("url");

        String[] urlsAndDb = urlStr.split("/");

        urlStr = urlsAndDb[0];

        if(urlsAndDb.length == 2) {
            String db = urlsAndDb[1];
            params.put("db", db);
        }



        StringBuilder hosts = new StringBuilder();
        StringBuilder ports = new StringBuilder();

        String[] hostPortStrList = urlStr.split(",");
        for (String hostPortStr : hostPortStrList) {
            String[] hostPort = hostPortStr.split(":");
            hosts.append(hostPort[0].trim()).append(",");
            ports.append(hostPort[1].trim()).append(",");
        }

        hosts.replace(hosts.length() - 1, hosts.length(), "");
        ports.replace(ports.length() - 1, ports.length(), "");

        params.put("host", hosts.toString());
        params.put("port", ports.toString());

        params.put("dbType", dbType);
        params.put("url", urlStr);



        info.forEach((key, value) -> {
            if (StringUtils.equalsIgnoreCase(String.valueOf(key), "user")) {
                params.put("userName", info.getProperty("user"));
            } else if (StringUtils.equalsIgnoreCase(String.valueOf(key), "password")) {
                params.put("password", info.getProperty("password"));
            } else {
                params.put(String.valueOf(key), value);
            }
        });

        //获取bean
        DbResourceEnum dbResourceEnum = DbResourceEnum.findDbResourceEnumByName(dbType);
        DbModuleService bean = DbBeanManagerUtil.getBean(GetBeanNameUtil.getDbServiceBeanName(dbResourceEnum), DbModuleService.class);

        if(bean == null) {
            throw new RuntimeException("dat[" + dbType + "]驱动不存在，请检查是否引入了对应的依赖包");
        }

        try {
            //组装数据库连接信息
            BaseDbConfTemplate baseDbConfTemplate = DatabaseConfFactory.getInstance(dbResourceEnum, params);
            return new DatConnection(url, info.getProperty("user"),(String) params.get("db"), baseDbConfTemplate, bean, dbResourceEnum);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("数据库连接信息获取失败");
            throw new RuntimeException("数据库连接信息获取失败");
        }


    }

    /**
     * url合规性验证
     * @param url
     * @return
     * @throws SQLException
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if(StringUtils.isBlank(url)) {
            return false;
        }

        Matcher matcher = SCHEME_PTRN.matcher(url);
        if(!matcher.matches()) {
            return false;
        }

        String scheme = matcher.group("scheme");
        return scheme.startsWith(SCHEME) && url.contains("dat");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }
}
