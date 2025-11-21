package com.meiya.whalex.business.entity;

import lombok.Data;

/**
 * 证书实体
 *
 * @author 黄河森
 * @date 2020/4/7
 * @project whale-cloud-platformX
 */
@Data
public class CertificateConf {

    private String keyId;

    private String certificateName;

    private String createTime;

    private byte[] content;

    private String certificateStatus;

    private String fileName;

    private String userName;

    public CertificateConf() {
    }

    public CertificateConf(byte[] content, String fileName, String userName) {
        this.content = content;
        this.fileName = fileName;
        this.userName = userName;
    }
}
