package com.meiya.whalex.db.template.graph;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author chenjp
 * @date 2020/9/27
 *
 * Neo4j 图数据库模板
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Neo4jDbTemplate {

    /**
     * 服务地址
     */
    private String serviceUrl;


    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

}
