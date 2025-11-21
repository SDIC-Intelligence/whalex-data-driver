package com.meiya.whalex.db.entity.cache;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * redis 组件数据库配置信息
 *
 * @author chenjp
 * @date 2020/9/7
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class RedisDatabaseInfo extends AbstractDatabaseInfo {

    public static final String KERBEROS_AUTH = "cer";
    public static final String PASSWORD_AUTH = "password";

    private List<String> serverUrl;

    private String username;

    private String password;

    private Integer dbIndex;

    private String certificateId;

    private String serviceType;

    private String parentDir;

    private String authType;

    private String certificateBase64Str;

    private String krb5Path;

    private String userKeytabPath;

    private boolean useLock;

    @Override
    public String getServerAddr() {
        if (CollectionUtils.isNotEmpty(serverUrl)) {
            return StringUtils.replaceEach(serverUrl.toString(), new String[]{"[", "]"}, new String[]{"", ""});
        }
        return null;
    }

    @Override
    public String getDbName() {
        return dbIndex == null ? "0" : dbIndex.toString();
    }


}
