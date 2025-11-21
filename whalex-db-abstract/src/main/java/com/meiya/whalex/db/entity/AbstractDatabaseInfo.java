package com.meiya.whalex.db.entity;

import com.meiya.whalex.util.JsonUtil;

import java.io.Serializable;

/**
 * 数据库信息基类
 *
 * @author 黄河森
 * @date 2019/11/10
 * @project whale-cloud-platformX
 */
public abstract class AbstractDatabaseInfo implements Cloneable, Serializable {

    protected static final String SERVER_ADDR_TEMP = "%s:%s";

    /**
     * 数据库地址
     *
     * @return
     */
    public abstract String getServerAddr();

    /**
     * 获取数据库名
     *
     * @return
     */
    public abstract String getDbName();

    @Override
    public Object clone() throws CloneNotSupportedException {
        String objectToStr = JsonUtil.objectToStr(this);
        return JsonUtil.jsonStrToObject(objectToStr, this.getClass());
    }

}
