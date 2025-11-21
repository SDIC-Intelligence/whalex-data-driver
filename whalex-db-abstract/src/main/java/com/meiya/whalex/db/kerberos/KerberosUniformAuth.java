package com.meiya.whalex.db.kerberos;

import cn.hutool.core.io.FileUtil;
import com.meiya.whalex.util.io.FindFileUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * kerberos 认证登录对象
 *
 * @author 黄河森
 * @date 2023/5/29
 * @package com.meiya.whalex.db.kerberos
 * @project whalex-data-driver
 */
@Data
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class KerberosUniformAuth {

    /**
     * 用户名
     */
    private String userName;

    /**
     * kerberos 认证主体名称
     */
    private String kerberosPrincipal;

    /**
     * krb5 地址
     */
    private String krb5Path;
    /**
     * userKeytab 地址
     */
    private String userKeytabPath;

    /**
     * krb5文件二进制字节码
     */
    private byte[] krb5;

    /**
     * keyTab 文件二进制字节码
     */
    private byte[] keytab;

    /**
     * 证书上级目录
     */
    private String parentDir;

    /**
     * 刷新认证方法
     */
    private RefreshKerberos refreshKerberos;

    /**
     * 认证登录
     *
     * @param module
     * @throws IOException
     */
    public void login(KerberosJaasConfigurationUtil.Module module) throws Exception {
        login(module, null);
    }

    /**
     * 扩展登录
     *
     * @param callBack
     * @param module
     * @throws Exception
     */
    public void login(KerberosJaasConfigurationUtil.Module module, KerberosLoginCallBack callBack) throws Exception {
        // 初始化证书配置信息
        initAuthConfig();
        if (module != null) {
            // 初始化 jaas 文件
            KerberosJaasConfigurationUtil.setJaasConfig(userName, userKeytabPath, module);
        }
        // 初始化 krb5 文件
        KerberosEnvConfigurationUtil.setKrb5Config(krb5Path, krb5);
        if (callBack != null) {
            // 执行回调函数
            callBack.loginExtend(this);
        }
    }

    /**
     * 初始化认证配置
     *
     * @throws IOException
     */
    public void initAuthConfig() throws IOException {
        if (StringUtils.isBlank(krb5Path)) {
            krb5Path = FindFileUtil.getFilePath("krb5.conf");
            if (krb5Path == null) {
                throw new IOException("没找到 krb5.conf 文件");
            }
        }
        if (StringUtils.isBlank(userKeytabPath)) {
            userKeytabPath = FindFileUtil.getFilePath("user.keytab");
            if (userKeytabPath == null) {
                throw new IOException("没找到 user.keytab 文件");
            }
        }
        if (userName == null) {
            List<String> list = FileUtil.readLines(userKeytabPath, Charset.defaultCharset());
            userName = list.get(list.size() - 1).split("[\u0000-\u0020]+")[1].trim();
        }
    }

    /**
     * 获取 kerberos principal
     * 如果当前未设置则从krb5文件中解析
     *
     * @return
     */
    public String getPrincipalOrKrb5() {
        if (StringUtils.isBlank(kerberosPrincipal)) {
            String _kerberosPrincipal = "hadoop.hadoop.com";
            try {
                for (String nLine : FileUtil.readLines(krb5Path, Charset.defaultCharset())) {
                    if (StringUtils.isBlank(nLine)) {
                        continue;
                    }
                    nLine = StringUtils.trim(nLine).toUpperCase();
                    if (nLine.startsWith("DEFAULT_REALM")) {
                        _kerberosPrincipal = "hadoop." + StringUtils.trim(nLine.split("=")[1]);
                        _kerberosPrincipal = _kerberosPrincipal.toLowerCase();
                        break;
                    }
                }
            } catch (Exception e) {
            }
            return _kerberosPrincipal;
        } else {
            return kerberosPrincipal;
        }
    }

    /**
     * 刷新认证
     */
    public void refresh() throws Exception {
        if (refreshKerberos != null) {
            refreshKerberos.refresh(this);
        }
    }

    /**
     * 刷新认证
     */
    public interface RefreshKerberos {
        /**
         * 刷新认证方法
         *
         * @param kerberosUniformLogin
         * @throws Exception
         */
        void refresh(KerberosUniformAuth kerberosUniformLogin) throws Exception;
    }
}
