package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 * @description NebulaGraphDatabaseInfo
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class NebulaGraphDatabaseInfo extends AbstractDatabaseInfo {

    private List<String> serverUrl;

    private String username;

    private String password;

    private String spaceName;


    @Override
    public String getServerAddr() {
        if (CollectionUtils.isNotEmpty(serverUrl)) {
            return StringUtils.replaceEach(serverUrl.toString(), new String[]{"[", "]"}, new String[]{"", ""});
        }
        return null;
    }

    @Override
    public String getDbName() {
        return spaceName;
    }
}
