/*
package com.meiya.whalex.db.util.helper.impl.graph;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.meiya.whalex.db.exception.GetCertificateException;
import com.meiya.whalex.db.exception.KerberosLoginException;
import com.meiya.whalex.util.io.ApacheZipUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

*/
/**
 * @author 黄河森
 * @date 2020/9/24
 * @project whalex-data-driver
 *//*

@Slf4j
public class KerberosUniformLoginFactory {

    */
/**
     * 证书压缩包类型
     *//*

    private static final String CERTIFICATE_FILE_TYPE = ".zip";

    */
/**
     * 证书缓存
     *//*

    private static Cache<String, KerberosUniformLogin> CERTIFICATE_CACHE = CacheBuilder.newBuilder().build();

    */
/**
     * 从本地配置文件取华为认证信息
     *
     * @return
     *//*

    public static KerberosUniformLogin getCertificateByKeyId(String userName, String krb5Path, String userKeytabPath) {
        log.info("从本地配置文件取华为认证信息");
        return new KerberosUniformLogin(userName, krb5Path, userKeytabPath);
    }

    */
/**
     * 从数据库取华为信息
     *
     * @param keyId
     * @return
     *//*

    public static KerberosUniformLogin getCertificateByKeyId(final String keyId, final byte[] content, final String fileName, final String userName) throws GetCertificateException {
        log.info("准备获取华为认证,加载谁认证 keyId：[{}]", keyId);
        try {
            return CERTIFICATE_CACHE.get(keyId, () -> getKerberosUniformLogin(keyId, content, fileName, userName));
        } catch (ExecutionException e) {
            throw new GetCertificateException("取证书回调失败." + e.getMessage(), e);
        }
    }

    */
/**
     * 解析证书
     *
     * @param keyId
     * @param content
     * @param fileName
     * @param userName
     * @return
     * @throws GetCertificateException
     *//*

    public static KerberosUniformLogin getKerberosUniformLogin(String keyId, byte[] content, String fileName, String userName) throws GetCertificateException {
        log.info("加载 证书 keyId：[{}]", keyId);
        if (content == null) {
            throw new GetCertificateException("根据id查询证书失败,找不到记录.");
        }
        if (!fileName.endsWith(CERTIFICATE_FILE_TYPE)) {
            throw new GetCertificateException("不支持除 .zip 之外的其它格式");
        }
        try {
            ApacheZipUtil apacheZipUtil = new ApacheZipUtil(new ByteArrayInputStream(content));
            byte[] krb5 = apacheZipUtil.getFileByte("krb5.conf");
            byte[] keytab = apacheZipUtil.getFileByte("user.keytab");
            if (krb5 == null && keytab == null) {
                throw new KerberosLoginException("压缩包里,没找到认证文件 krb5.conf user.keytab");
            }
            if (krb5 == null) {
                throw new KerberosLoginException("压缩包里,没找到认证文件 krb5.conf ");
            }
            if (keytab == null) {
                throw new KerberosLoginException("压缩包里,没找到认证文件 user.keytab ");
            }
            List<String> keytabList = IOUtils.readLines(new ByteArrayInputStream(keytab));
            if (StringUtils.isEmpty(userName)) {
                String[] lastLineArray = keytabList.get(keytabList.size() - 1).split("[\u0000-\u0020]+");
                if (lastLineArray.length < 2) {
                    throw new KerberosLoginException("未获取到认证用户");
                }
                userName = lastLineArray[1].trim();
            }

            String tmpDir = System.getProperty("java.io.tmpdir");
            if (!tmpDir.endsWith(File.separator)) {
                tmpDir = tmpDir + File.separator;
            }
            String userBaseDir = tmpDir + System.getProperty("user.name");
            if (!new File(userBaseDir).exists()) {
                new File(userBaseDir).mkdirs();
            }
            String authBaseDir = tmpDir + System.getProperty("user.name")
                    + File.separator + StringUtils.replaceEach(userName, new String[]{"\\", "/", "@"}, new String[]{"", "", ""}) + File.separator;
            if (!new File(authBaseDir).exists()) {
                new File(authBaseDir).mkdirs();
            }
            String krb5Path = authBaseDir + "krb5.conf";
            String usertabPath = authBaseDir + "user.keytab";

            IOUtils.write(krb5, new FileOutputStream(krb5Path));
            IOUtils.write(keytab, new FileOutputStream(usertabPath));
            return new KerberosUniformLogin(userName, krb5Path, usertabPath);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new GetCertificateException("解压失败，请检查证书格式." + e.getMessage(), e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new GetCertificateException("取证书失败." + e.getMessage(), e);
        }
    }

}
*/
