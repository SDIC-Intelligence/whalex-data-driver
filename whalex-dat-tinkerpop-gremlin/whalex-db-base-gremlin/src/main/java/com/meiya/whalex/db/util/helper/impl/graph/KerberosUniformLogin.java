/*
package com.meiya.whalex.db.util.helper.impl.graph;

import cn.hutool.core.io.FileUtil;
import com.meiya.whalex.db.kerberos.KerberosJaasConfigurationUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.util.UUIDGenerator;
import com.meiya.whalex.util.io.FindFileUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

*/
/**
 * @author 黄河森
 * @date 2020/9/24
 * @project whalex-data-driver
 *//*

@Data
@Slf4j
public class KerberosUniformLogin {

    */
/**
     * 认证文件配置类路径
     *//*

    private final static String CLASS_PATH = "classpath:";

    */
/**
     * 用户名
     *//*

    private String userName;
    */
/**
     * krb5 地址
     *//*

    private String krb5Path;
    */
/**
     * userKeytab 地址
     *//*

    private String userKeytabPath;

    */
/**
     * 初始化环境变量
     *
     * @throws IOException
     *//*

    public void login() throws IOException {
        initAuthConfig();
        KerberosJaasConfigurationUtil.setJaasConfig(userName, userKeytabPath, KerberosJaasConfigurationUtil.Module.GREMLIN);
        KerberosLoginUtil.setKrb5Config(krb5Path);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    }

    */
/**
     * 初始化认证配置
     *
     * @throws IOException
     *//*

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
            try(FileInputStream fileInputStream = new FileInputStream(userKeytabPath)) {
                List<String> list = IOUtils.readLines(fileInputStream);
                userName = list.get(list.size() - 1).split("[\u0000-\u0020]+")[1].trim();
            }finally {}
        }
    }

    public KerberosUniformLogin() {
    }

    public KerberosUniformLogin(String userName, String krb5Path, String userKeytabPath) {
        this.userName = userName;
        this.krb5Path = classFileToUserDir(krb5Path, userName);
        this.userKeytabPath = classFileToUserDir(userKeytabPath, userName);
    }



    public static String classFileToUserDir(String path, String userName) {
        if (StringUtils.startsWithIgnoreCase(path, CLASS_PATH)) {
            InputStream inputStream = null;
            try {
                if (StringUtils.isBlank(userName)) {
                    userName = UUIDGenerator.shortGenerate();
                }
                String fileName = StringUtils.trim(StringUtils.substringAfter(path, CLASS_PATH));
                URL resource = FindFileUtil.class.getClassLoader().getResource(fileName);
                inputStream = resource.openConnection().getInputStream();
                String tmpDir = System.getProperty("java.io.tmpdir");
                if (!tmpDir.endsWith(File.separator)) {
                    tmpDir = tmpDir + File.separator;
                }
                String userBaseDir = tmpDir + System.getProperty("user.name");
                if (!new File(userBaseDir).exists()) {
                    new File(userBaseDir).mkdirs();
                }
                String authBaseDir = tmpDir + System.getProperty("user.name")
                        + File.separator + userName + File.separator;
                if (!new File(authBaseDir).exists()) {
                    new File(authBaseDir).mkdirs();
                }
                String file = authBaseDir + fileName;
                FileUtil.writeFromStream(inputStream, file);
                return file;
            } catch (IOException e) {
                throw new BusinessException("将classpath路径下的文件传输到用户临时目录下失败!", e);
            } finally {
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        log.error("关闭认证文件流对象失败!", e);
                    }
                }
            }
        } else {
            return path;
        }
    }
}
*/
