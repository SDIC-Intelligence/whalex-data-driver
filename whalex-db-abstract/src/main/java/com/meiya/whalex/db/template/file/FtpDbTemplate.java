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
 * @author wangkm1
 * @date 2020/7/6
 * @project whalex-data-driver
 */
@Data
@Builder
@AllArgsConstructor
@DbType(value = {DbResourceEnum.ftp})
public class FtpDbTemplate extends BaseDbConfTemplate {
    /**
     * ip:port
     */
    @Url
    private String serviceUrl;


    /**
     * 用户名
     */
    @ExtendField(value = "userName")
    private String userName;


    /**
     * 密码
     */
    @ExtendField(value = "password")
    private String password;

    /**
     * 认证类型(如果是认证文件认证,填写cer)
     */
    @ExtendField(value = "authType")
    private String authType;

    /**
     * 编码
     */
    @ExtendField(value = "controlEncoding")
    private String controlEncoding;






    /**
     * 基础路径
     */
    @ExtendField(value = "bashPath")
    private String bashPath;


    public FtpDbTemplate() {
        // Default constructor
    }

    /**
     * 认证文件认证模式
     *
     * @param serviceUrl
     * @param certificateId
     * @param userName
     * @param password
     */
    public FtpDbTemplate(String serviceUrl,  String userName, String password) {
        this.serviceUrl = serviceUrl;
        this.userName = userName;
        this.password = password;

    }

    public FtpDbTemplate(String serviceUrl, String userName, String authType, String password) {
        this.serviceUrl = serviceUrl;
        this.userName = userName;
        this.authType = authType;
        this.password = password;
    }

}
