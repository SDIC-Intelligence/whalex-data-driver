package com.meiya.whalex.db.template.graph;

import com.meiya.whalex.annotation.*;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @author 黄河森
 * @date 2024/3/11
 * @package com.meiya.whalex.db.template.graph
 * @project whalex-data-driver
 * @description NebulaGraphDbTemplate
 */
@Builder
@DbType(value = {DbResourceEnum.nebulagraph})
@AllArgsConstructor
@Data
public class NebulaGraphDbTemplate {

    /**
     * 服务地址
     */
    @Url
    private String serviceUrl;


    /**
     * 用户名
     */
    @UserName
    private String username;

    /**
     * 密码
     */
    @Password
    private String password;

    /**
     * 空间名称
     */
    @DatabaseName
    private String spaceName;

}
