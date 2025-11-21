package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * neo4j 组件数据库配置信息
 *
 * @author chenjp
 * @date 2020/9/27
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class Neo4jDatabaseInfo extends AbstractDatabaseInfo {

    private List<String> serverUrl;

    private String username;

    private String password;

    private String database;

    @Override
    public String getServerAddr() {
        if (CollectionUtils.isNotEmpty(serverUrl)) {
            return StringUtils.replaceEach(serverUrl.toString(), new String[]{"[", "]"}, new String[]{"", ""});
        }
        return null;
    }

    @Override
    public String getDbName() {
        return database;
    }
}
