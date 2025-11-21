package com.meiya.whalex.db.ani;

import com.meiya.whalex.db.entity.ani.BaseMySqlDatabaseInfo;
import lombok.Data;

/**
 * GBase 组件数据库配置信息
 *
 * @author 蔡荣桂
 * @date 2022/05/23
 * @project whale-cloud-platformX
 */
@Data
public class GBaseDatabaseInfo extends BaseMySqlDatabaseInfo {

    /**
     * 连接地址模板
     */
    public static String URL_TEMPLATE = "jdbc:gbase://%s:%s/%s?useUnicode=true&characterEncoding=%s&autoReconnect=true&autoReconnectForPools=true";

    /**
     * 驱动类
     */
    public static String DRIVER_CLASS_NAME = "com.gbase.jdbc.Driver";

}
