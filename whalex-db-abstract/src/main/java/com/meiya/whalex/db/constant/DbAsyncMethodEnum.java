package com.meiya.whalex.db.constant;

import com.meiya.whalex.db.entity.DbMergeEntity;
import com.meiya.whalex.db.entity.DbQueryEntity;

/**
 * 异步请求方法枚举
 *
 * @author xult
 * @date 2020/3/16
 * @project whale-cloud-platformX
 */
public enum DbAsyncMethodEnum {

    /**
     * 统计数据量
     */
    statisticalLine("com.meiya.whalex.db.api.DbQueryApi#statisticalLine", DbQueryEntity.class),
    mergeData("com.meiya.whalex.db.api.DbQueryApi#mergeData", DbMergeEntity.class),
    queryListForPage("com.meiya.whalex.db.api.DbQueryApi#queryListForPage", DbQueryEntity.class),
    ;

    /**
     * 方法名称
     */
    private String method;

    /**
     * 方法参数
     */
    private Class<?>[] parameterTypes;

    DbAsyncMethodEnum(String method, Class<?>... parameterTypes) {
        this.method = method;
        this.parameterTypes = parameterTypes;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Class<?>[] getParameterTypes() {
        return parameterTypes;
    }

    public void setParameterTypes(Class<?>[] parameterTypes) {
        this.parameterTypes = parameterTypes;
    }
}
