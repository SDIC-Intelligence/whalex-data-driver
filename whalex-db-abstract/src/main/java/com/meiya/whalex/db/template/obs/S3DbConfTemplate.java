package com.meiya.whalex.db.template.obs;

import com.meiya.whalex.db.template.BaseDbConfTemplate;
import lombok.*;

/**
 * s3 模板配置
 *
 * @author xult
 * @date 2020/3/25
 * @project whale-cloud-platformX
 */
@Data
@AllArgsConstructor
@ToString
@Builder
@NoArgsConstructor
public class S3DbConfTemplate extends BaseDbConfTemplate {

    /**
     * 域名
     */
    private String endpoint;
    /**
     * 数据中心区域
     */
    private String region;
    /**
     * 接入键标识
     */
    private String accessKey;
    /**
     * 安全接入键
     */
    private String secretKey;

    /**
     * 凭证验证方式
     */
    private String signerOverride;
    /**
     * 访问协议
     */
    private String protocol;

}
