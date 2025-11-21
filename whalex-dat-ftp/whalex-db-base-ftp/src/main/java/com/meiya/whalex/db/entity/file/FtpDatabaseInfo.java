package com.meiya.whalex.db.entity.file;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * 数据库配置信息
 *
 * @author wangkm1
 * @date 2025/03/31
 * @project whale-cloud-platformX
 */
@Builder
@AllArgsConstructor
@Data
public class FtpDatabaseInfo extends AbstractDatabaseInfo {

    /**
     * HDfs地址
     */
    private String serviceUrl;

    /**
     * 认证类型
     */
    private String authType;

    /**
     * 编码
     */
    private String controlEncoding;

    /**
     * 用户名
     */
    private String userName;

    /**
     * 密码
     */
    private String password;
    /**
     * 基础路径
     */
    private String bashPath;



    public FtpDatabaseInfo() {
    }

    public FtpDatabaseInfo(String serviceUrl, String authType, String certificateId, String userName, String password) {
        this.serviceUrl = serviceUrl;
        this.authType = authType;
        this.userName = userName;
        this.password = password;

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
