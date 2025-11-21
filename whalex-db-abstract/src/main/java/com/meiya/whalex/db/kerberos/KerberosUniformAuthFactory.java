package com.meiya.whalex.db.kerberos;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.meiya.whalex.db.kerberos.exception.GetKerberosException;
import com.meiya.whalex.db.kerberos.exception.LoginKerberosException;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.util.concurrent.ThreadNamedFactory;
import com.meiya.whalex.util.io.ApacheZipUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * kerberos 认证登录对象工厂
 *
 * @author 黄河森
 * @date 2023/5/29
 * @package com.meiya.whalex.db.kerberos
 * @project whalex-data-driver
 */
@Slf4j
public class KerberosUniformAuthFactory {

    /**
     * 证书压缩包类型
     */
    private static final String CERTIFICATE_FILE_TYPE = ".zip";

    /**
     * 证书缓存
     */
    private static Cache<String, KerberosUniformAuth> KERBEROS_CACHE = CacheBuilder.newBuilder().build();

    /**
     * 定时任务，认证不超时机制
     */
    private static ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = null;

    /**
     * 存放需要重新认证的 kerberos
     */
    private static Set<String> RE_LOGIN_KERBEROS_KEY = new HashSet<>();

    /**
     * 从本地配置文件取华为认证信息
     *
     * @param dbType
     * @param userName
     * @param krb5Path
     * @param userKeytabPath
     * @param parentPath
     * @param refreshKerberos
     * @return
     * @throws GetKerberosException
     */
    public static KerberosUniformAuth getKerberosUniformLoginByLocal(DbResourceEnum dbType, DbVersionEnum version, CloudVendorsEnum cloud, String userName, String kerberosPrincipal, String krb5Path, String userKeytabPath, String parentPath, KerberosUniformAuth.RefreshKerberos refreshKerberos) throws GetKerberosException {
        if (!FileUtil.exist(krb5Path) || !FileUtil.exist(userKeytabPath)) {
            throw new GetKerberosException("krb5 或者 keytab 文件不存在!");
        }
        try {
            if (StringUtils.isBlank(userName)) {
                List<String> keytabList = FileUtil.readLines(userKeytabPath, Charset.defaultCharset());
                String[] lastLineArray = keytabList.get(keytabList.size() - 1).split("[\u0000-\u0020]+");
                if (lastLineArray.length < 2) {
                    throw new LoginKerberosException("未获取到认证用户");
                }
                userName = lastLineArray[1].trim();
            }
            parentPath = getPidParentPath(parentPath);
            return getKerberosUniformLoginByCache(dbType, version, cloud, userName, kerberosPrincipal, krb5Path, userKeytabPath, FileUtil.readBytes(krb5Path), FileUtil.readBytes(userKeytabPath), parentPath
                    , refreshKerberos
                    , new KerberosUniformLoginCallBack() {
                @Override
                public KerberosUniformAuth create(String username, String kerberosPrincipal, String krb5Path, String userTabPath, byte[] krb5, byte[] keytab, String parentPath, KerberosUniformAuth.RefreshKerberos refreshKerberos) throws Exception {
                    return new KerberosUniformAuth(username, kerberosPrincipal, krb5Path, userKeytabPath, krb5, keytab, parentPath, refreshKerberos);
                }
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new GetKerberosException("取证书失败." + e.getMessage(), e);
        }
    }

    /**
     * 解析证书
     *
     * @param dbType
     * @param content
     * @param fileName
     * @param userName
     * @param parentPath
     * @param refreshKerberos
     * @return
     * @throws GetKerberosException
     */
    public static KerberosUniformAuth getKerberosUniformLoginByZip(DbResourceEnum dbType, DbVersionEnum version, CloudVendorsEnum cloud, byte[] content, String fileName, String userName, String kerberosPrincipal, String parentPath, KerberosUniformAuth.RefreshKerberos refreshKerberos) throws GetKerberosException {
        if (content == null) {
            throw new GetKerberosException("zip认证包为空.");
        }
        if (!fileName.endsWith(CERTIFICATE_FILE_TYPE)) {
            throw new GetKerberosException("不支持除 .zip 之外的其它格式");
        }
        try {
            ApacheZipUtil apacheZipUtil = new ApacheZipUtil(new ByteArrayInputStream(content));
            byte[] krb5 = apacheZipUtil.getFileByte("krb5.conf");
            byte[] keytab = apacheZipUtil.getFileByte("user.keytab");
            if (krb5 == null && keytab == null) {
                throw new LoginKerberosException("压缩包里,没找到认证文件 krb5.conf user.keytab");
            }
            if (krb5 == null) {
                throw new LoginKerberosException("压缩包里,没找到认证文件 krb5.conf ");
            }
            if (keytab == null) {
                throw new LoginKerberosException("压缩包里,没找到认证文件 user.keytab ");
            }
            if (StringUtils.isBlank(userName)) {
                List<String> keytabList;
                InputStreamReader inputStreamReader = null;
                try {
                    inputStreamReader = new InputStreamReader(new ByteArrayInputStream(keytab), Charset.defaultCharset());
                    keytabList = IoUtil.readLines(inputStreamReader, CollectionUtil.newArrayList());
                } finally {
                    IoUtil.close(inputStreamReader);
                }
                String[] lastLineArray = keytabList.get(keytabList.size() - 1).split("[\u0000-\u0020]+");
                if (lastLineArray.length < 2) {
                    throw new LoginKerberosException("未获取到认证用户");
                }
                userName = lastLineArray[1].trim();
            }

            parentPath = getPidParentPath(parentPath);
            return getKerberosUniformLoginByCache(dbType, version, cloud, userName, kerberosPrincipal
                    , null, null, krb5, keytab, parentPath, refreshKerberos, new KerberosUniformLoginCallBack() {
                        @Override
                        public KerberosUniformAuth create(String username
                                , String kerberosPrincipal
                                , String krb5Path
                                , String userTabPath
                                , byte[] krb5
                                , byte[] keytab
                                , String parentPath
                                , KerberosUniformAuth.RefreshKerberos refreshKerberos) throws Exception {
                            KerberosFileCreate kerberosFileCreate = new KerberosFileCreate(username, krb5, keytab, parentPath).invoke();
                            krb5Path = kerberosFileCreate.getKrb5Path();
                            userTabPath = kerberosFileCreate.getUserTabPath();
                            return new KerberosUniformAuth(username, kerberosPrincipal, krb5Path, userTabPath, krb5, keytab, parentPath, refreshKerberos);
                        }
                    });
        } catch (IOException e) {
            throw new GetKerberosException("解压失败，请检查证书格式." + e.getMessage(), e);
        } catch (Exception e) {
            throw new GetKerberosException("取证书失败." + e.getMessage(), e);
        }
    }

    /**
     * 根据当前线程ID生成对应的根目录
     *
     * @param pidParentPath
     * @return
     */
    private static String getPidParentPath(String pidParentPath) {
        // 若未设置保存路径，则设置在临时目录下
        if (StringUtils.isBlank(pidParentPath)) {
            String tmp = System.getProperty("java.io.tmpdir");
            if (!tmp.endsWith(File.separator)) {
                tmp = tmp + File.separator;
            }
            pidParentPath = tmp + System.getProperty("user.name");
        }
        if (!pidParentPath.endsWith(File.separator)) {
            pidParentPath = pidParentPath + File.separator;
        }
        // 设置当前进程号为最后层级文件夹名称
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        String name = runtimeMXBean.getName();
        String pid = StringUtils.substringBefore(name, "@");
        pidParentPath = pidParentPath + pid;
        if (!new File(pidParentPath).exists()) {
            new File(pidParentPath).mkdirs();
        }
        if (!pidParentPath.endsWith(File.separator)) {
            pidParentPath = pidParentPath + File.separator;
        }
        return pidParentPath;
    }

    /**
     * 证书缓存获取操作
     *
     * @param dbType
     * @param username
     * @param krb5Path
     * @param userTabPath
     * @param krb5
     * @param keytab
     * @param parentPath
     * @param refreshKerberos
     * @param callBack
     * @return
     * @throws ExecutionException
     */
    private static KerberosUniformAuth getKerberosUniformLoginByCache(DbResourceEnum dbType, DbVersionEnum version, CloudVendorsEnum cloud, String username
            , String kerberosPrincipal
            , String krb5Path
            , String userTabPath
            , byte[] krb5
            , byte[] keytab
            , String parentPath
            , KerberosUniformAuth.RefreshKerberos refreshKerberos
            , KerberosUniformLoginCallBack callBack) throws ExecutionException {
        String key = dbType.name() + "-" + version.getVersion() + "-" + cloud.name() + "-" + krb5.hashCode() + "-" + keytab.hashCode();
        KerberosUniformAuth kerberosUniformLogin = KERBEROS_CACHE.get(key, () -> callBack.create(username, kerberosPrincipal
                , krb5Path, userTabPath, krb5, keytab, parentPath, refreshKerberos));
        if (refreshKerberos != null) {
            RE_LOGIN_KERBEROS_KEY.add(key);
            if (scheduledThreadPoolExecutor == null) {
                startReKerberosAuthTask();
            }
        }
        return kerberosUniformLogin;
    }

    /**
     * kerberos keytab 和 conf 文件写本地
     */
    private static class KerberosFileCreate {
        private String userName;
        private byte[] krb5;
        private byte[] keytab;
        private String krb5Path;
        private String userTabPath;
        private String parentPath;


        public KerberosFileCreate(String userName, byte[] krb5, byte[] keytab, String parentPath) {
            this.userName = userName;
            this.krb5 = krb5;
            this.keytab = keytab;
            this.parentPath = parentPath;
        }

        public String getKrb5Path() {
            return krb5Path;
        }

        public String getUserTabPath() {
            return userTabPath;
        }

        public KerberosFileCreate invoke() throws Exception {
            if (StringUtils.isBlank(parentPath)) {
                throw new LoginKerberosException("必须定义证书根目录信息!");
            }
            if (!parentPath.endsWith(File.separator)) {
                parentPath = parentPath + File.separator;
            }
            String authBaseDir = parentPath
                    + StringUtils.replaceEach(userName, new String[]{"\\", "/", "@"}, new String[]{"", "", ""}) + File.separator;
            if (!new File(authBaseDir).exists()) {
                new File(authBaseDir).mkdirs();
            }
            krb5Path = authBaseDir + "krb5.conf";
            userTabPath = authBaseDir + "user.keytab";
            FileUtil.writeBytes(krb5, krb5Path);
            FileUtil.writeBytes(keytab, userTabPath);
            return this;
        }
    }

    public interface KerberosUniformLoginCallBack {

        /**
         * 创建 KerberosUniformLogin
         *
         * @param username
         * @param kerberosPrincipal
         * @param krb5Path
         * @param userTabPath
         * @param krb5
         * @param keytab
         * @param parentPath
         * @param refreshKerberos
         * @return
         * @throws Exception
         */
        KerberosUniformAuth create(String username
                , String kerberosPrincipal
                , String krb5Path
                , String userTabPath
                , byte[] krb5
                , byte[] keytab
                , String parentPath
                , KerberosUniformAuth.RefreshKerberos refreshKerberos) throws Exception;

    }

    /**
     * kerberos 认证定时重登录，防止超时
     */
    public static synchronized void startReKerberosAuthTask() {
        if (scheduledThreadPoolExecutor == null) {
            scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1, new ThreadNamedFactory("kerberos-reLogin-task"));
            scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> {
                try {
                    Iterator<String> iterator = RE_LOGIN_KERBEROS_KEY.iterator();
                    while (iterator.hasNext()) {
                        String kerberosKey = iterator.next();
                        KerberosUniformAuth kerberosUniformLogin = null;
                        try {
                            try {
                                kerberosUniformLogin = KERBEROS_CACHE.getIfPresent(kerberosKey);
                            } catch (NullPointerException nullPointerException) {
                                log.warn("kerberosKey : [{}] 对应的认证对象已经不存在，将从当前任务重剔除!", kerberosKey);
                                RE_LOGIN_KERBEROS_KEY.remove(kerberosKey);
                                continue;
                            }
                            // 校验认证文件是否依旧存在
                            String krb5Path = kerberosUniformLogin.getKrb5Path();
                            String userKeytabPath = kerberosUniformLogin.getUserKeytabPath();
                            if (!FileUtil.exist(krb5Path) || !FileUtil.exist(userKeytabPath)) {
                                if (kerberosUniformLogin.getKrb5() == null || kerberosUniformLogin.getKeytab() == null) {
                                    log.warn("Kerberos 重新认证校验到认证文件已经不存在，并且文件二进制流未保存或者设置，可能导致重新认证失败!!!");
                                } else {
                                    log.warn("Kerberos 重新认证校验到认证文件已经不存在，重新将文件二进制流写入到本地文件中!!!");
                                    new KerberosFileCreate(kerberosUniformLogin.getUserName(), kerberosUniformLogin.getKrb5(), kerberosUniformLogin.getKeytab(), kerberosUniformLogin.getParentDir()).invoke();
                                }
                            }
                            kerberosUniformLogin.refresh();
                        } catch (Exception e) {
                            log.error("定时进行 kerberos TGT 检查并重新认证失败!");
                        }
                    }
                } catch (Exception e) {
                    log.error("定时进行Kerberos重新登录操作失败!", e);
                }
            }, 1, 1, TimeUnit.HOURS);
        }
    }

}
