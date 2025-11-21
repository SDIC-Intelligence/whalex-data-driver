package com.meiya.whalex.db.resolver;

import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.exception.BusinessException;

/**
 * 统计结果解析器
 *
 * @author 黄河森
 * @date 2021/6/21
 * @project whalex-data-driver-back
 */
public class CountResolver {

    private PageResult pageResult;

    private CountResolver(PageResult pageResult) {
        this.pageResult = pageResult;
    }

    /**
     * 解析器
     *
     * @param pageResult
     * @return
     */
    public static CountResolver resolver(PageResult pageResult) {
        return new CountResolver(pageResult);
    }

    /**
     * 解析方法
     *
     * @return
     */
    public Long analysis() {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        return this.pageResult.getTotal();
    }
}
