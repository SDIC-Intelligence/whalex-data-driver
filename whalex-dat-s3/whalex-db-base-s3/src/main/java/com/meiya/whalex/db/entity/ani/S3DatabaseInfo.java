package com.meiya.whalex.db.entity.ani;

import com.amazonaws.Protocol;
import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import lombok.Data;
import lombok.ToString;

import javax.validation.constraints.NotBlank;

/**
 * PostGre 组件数据库配置信息
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Data
@ToString
public class S3DatabaseInfo extends AbstractDatabaseInfo {
    /**
     * 域名
     */
    @NotBlank(message = "s3 endpoint不允许为空")
    private String endpoint;
    /**
     * 数据中心区域
     */
    @NotBlank(message = "s3 region不允许为空")
    private String region;
    /**
     * 接入键标识
     */
    @NotBlank(message = "s3 ak不允许为空")
    private String accessKey;
    /**
     * 安全接入键
     */
    @NotBlank(message = "s3 sk不允许为空")
    private String secretKey;

    /**
     * 凭证验证方式
     */
    private String signerOverride = "S3SignerType";
    /**
     * 访问协议
     */
    private String protocol = Protocol.HTTP.name();

    private String bucket;

    @Override
    public String getServerAddr() {
        return endpoint;
    }

    @Override
    public String getDbName() {
        return region;
    }
    
}
