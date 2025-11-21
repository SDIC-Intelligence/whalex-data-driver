package com.meiya.whalex.db.resolver;

import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.exception.BusinessException;

/**
 * 数据库表操作基础解析类
 *
 * @author 黄河森
 * @date 2021/10/6
 * @package com.meiya.whalex.db.resolver
 * @project whalex-data-driver
 */
public class OperationResolver {

    private PageResult pageResult;

    private OperationResolver(PageResult pageResult) {
        this.pageResult = pageResult;
    }

    /**
     * 解析器
     *
     * @param pageResult
     * @return
     */
    public static OperationResolver resolver(PageResult pageResult) {
        return new OperationResolver(pageResult);
    }

    /**
     * 影响条数
     *
     * @return
     */
    public Long total() {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        return this.pageResult.getTotal();
    }

    /**
     * 执行状态
     * @return
     */
    public boolean status() {
        return this.pageResult.getSuccess();
    }

    /**
     * 返回信息
     *
     * @return
     */
    public String errorMsg() {
        return this.pageResult.getMessage();
    }

}
