package com.meiya.whalex.db.entity.file;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * 数据库配置信息
 *
 * @author 黄河森
 * @date 2019/12/30
 * @project whale-cloud-platformX
 */
@Builder
@AllArgsConstructor
@Data
public class HadoopDatabaseInfo extends AbstractDatabaseInfo {

    /**
     * HDfs地址
     */
    private String serviceUrl;

    /**
     * 认证类型
     */
    private String authType;

    /**
     * 认证证书
     */
    private String certificateId;

    /**
     * 从数据库下载证书时，保存的父级目录
     */
    private String parentDir;

    /**
     * 用户名
     */
    private String userName;
    /**
     * krb5 地址
     */
    private String krb5Path;
    /**
     * userKeytab 地址
     */
    private String userKeytabPath;

    /**
     * kerberos.principal
     */
    private String kerberosPrincipal;

    /**
     * 集群名称
     */
    private String clusterName;

    /**
     * 节点名：nm0,nm1,nm2，与serviceUrl对应
     */
    private String nameNodes;

    /**
     * 基础路径
     */
    private String bashPath;

    private String certificateBase64Str;

    private String rpcProtection;

    public HadoopDatabaseInfo() {
    }

    public HadoopDatabaseInfo(String serviceUrl, String authType, String certificateId, String parentDir, String userName, String krb5Path, String userKeytabPath, String kerberosPrincipal, String clusterName, String nameNodes) {
        this.serviceUrl = serviceUrl;
        this.authType = authType;
        this.certificateId = certificateId;
        this.parentDir = parentDir;
        this.userName = userName;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.kerberosPrincipal = kerberosPrincipal;
        this.clusterName = clusterName;
        this.nameNodes = nameNodes;
    }

    @Override
    public String getServerAddr() {
        return serviceUrl;
    }

    @Override
    public String getDbName() {
        return null;
    }

}
