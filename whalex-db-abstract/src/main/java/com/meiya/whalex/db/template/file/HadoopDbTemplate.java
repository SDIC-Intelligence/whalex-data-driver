package com.meiya.whalex.db.template.file;

import com.meiya.whalex.annotation.DbType;
import com.meiya.whalex.annotation.ExtendField;
import com.meiya.whalex.annotation.Url;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @author 黄河森
 * @date 2020/7/6
 * @project whalex-data-driver
 */
@Data
@Builder
@AllArgsConstructor
@DbType(value = {DbResourceEnum.hadoop})
public class HadoopDbTemplate extends BaseDbConfTemplate {
    /**
     * ip:port
     */
    @Url
    private String serviceUrl;
    /**
     * 认证证书
     */
    @ExtendField(value = "certificateId")
    private String certificateId;
    /**
     * krb5 地址
     */
    @ExtendField(value = "krb5Path")
    private String krb5Path;
    /**
     * userKeytab 地址
     */
    @ExtendField(value = "userKeytabPath")
    private String userKeytabPath;

    /**
     * 用户名
     */
    @ExtendField(value = "userName")
    private String userName;

    /**
     * 认证类型(如果是认证文件认证,填写cer)
     */
    @ExtendField(value = "authType")
    private String authType;

    /*
    腾讯hdfs认证参数
     */
    @ExtendField(value = "secureid")
    private String secureid;
    @ExtendField(value = "securekey")
    private String securekey;

    /**
     * 集群名称
     */
    @ExtendField(value = "clusterName")
    private String clusterName;

    /**
     * 节点名：nm0,nm1,nm2，与serviceUrl对应
     */
    @ExtendField(value = "nameNodes")
    private String nameNodes;

    /**
     * 基础路径
     */
    @ExtendField(value = "bashPath")
    private String bashPath;

    @ExtendField(value = "certificateBase64Str")
    private String certificateBase64Str;

    /**
     * 从数据库下载证书时，保存的父级目录
     */
    @ExtendField(value = "parentDir")
    private String parentDir;

    /**
     * kerberos.principal
     */
    @ExtendField(value = "kerberosPrincipal")
    private String kerberosPrincipal;

    @ExtendField(value = "rpcProtection")
    private String rpcProtection;

    public HadoopDbTemplate() {
    }

    /**
     * 认证文件认证模式
     *
     * @param serviceUrl
     * @param krb5Path
     * @param userKeytabPath
     * @param userName
     */
    public HadoopDbTemplate(String serviceUrl, String krb5Path, String userKeytabPath, String userName) {
        this.serviceUrl = serviceUrl;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.userName = userName;
    }

    /**
     * 认证文件认证模式
     *
     * @param serviceUrl
     * @param certificateId
     * @param userName
     */
    public HadoopDbTemplate(String serviceUrl, String certificateId, String userName) {
        this.serviceUrl = serviceUrl;
        this.certificateId = certificateId;
        this.userName = userName;
    }

    public HadoopDbTemplate(String serviceUrl, String certificateId, String krb5Path, String userKeytabPath, String userName) {
        this.serviceUrl = serviceUrl;
        this.certificateId = certificateId;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.userName = userName;
    }


    public HadoopDbTemplate(String serviceUrl, String certificateId, String krb5Path, String userKeytabPath, String userName, String secureid, String securekey) {
        this.serviceUrl = serviceUrl;
        this.certificateId = certificateId;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.userName = userName;
        this.secureid = secureid;
        this.securekey = securekey;
    }

    public HadoopDbTemplate(String serviceUrl, String certificateId, String krb5Path, String userKeytabPath, String userName, String authType, String secureid, String securekey) {
        this.serviceUrl = serviceUrl;
        this.certificateId = certificateId;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.userName = userName;
        this.authType = authType;
        this.secureid = secureid;
        this.securekey = securekey;
    }

    public HadoopDbTemplate(String serviceUrl, String certificateId, String krb5Path, String userKeytabPath, String userName, String authType, String secureid, String securekey, String clusterName, String nameNodes) {
        this.serviceUrl = serviceUrl;
        this.certificateId = certificateId;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.userName = userName;
        this.authType = authType;
        this.secureid = secureid;
        this.securekey = securekey;
        this.clusterName = clusterName;
        this.nameNodes = nameNodes;
    }
}
