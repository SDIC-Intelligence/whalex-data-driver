package com.meiya.whalex.db.entity;

import com.meiya.whalex.interior.db.search.in.Page;
import lombok.Data;

/**
 * 库表游标查询缓存基础类
 *
 * @author xult
 * @date 2020/8/28
 * @project whale-cloud-platformX
 */
@Data
public abstract class AbstractCursorCache<Q extends AbstractDbHandler> {
    /**
     * 最后一次游标滚的分页情况
     */
    protected Page lastPage;
    /**
     * 滚游标的批量设置
     */
    protected Integer batchSize;

    /**
     * 组件查询
     */
    protected Q queryEntity;

    public AbstractCursorCache(Page lastPage, Integer batchSize) {
        this.lastPage = lastPage;
        this.batchSize = batchSize;
    }

    /**
     * 关闭游标方法
     */
    public abstract void closeCursor();
}
