package com.meiya.whalex.db.resolver;

import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.exception.BusinessException;

import java.util.Map;

/**
 * @author 黄河森
 * @date 2021/8/25
 * @project whalex-data-driver-back
 */
public class TableExistsResolver {

    private PageResult pageResult;

    private TableExistsResolver(PageResult pageResult) {
        this.pageResult = pageResult;
    }

    /**
     * 解析器
     *
     * @param pageResult
     * @return
     */
    public static TableExistsResolver resolver(PageResult pageResult) {
        return new TableExistsResolver(pageResult);
    }

    /**
     * 解析方法
     *
     * @return
     */
    public Boolean analysis() {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        return (Boolean) ((Map)this.pageResult.getRows().get(0)).values().iterator().next();
    }

}
