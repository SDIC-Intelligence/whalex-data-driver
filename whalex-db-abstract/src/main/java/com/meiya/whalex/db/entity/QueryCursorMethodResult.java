package com.meiya.whalex.db.entity;


import java.util.Collections;

/**
 * 组件内部查询方法统一返参
 *
 * @author 黄河森
 * @date 2019/9/14
 * @project whale-cloud-platformX
 */
public class QueryCursorMethodResult<T extends AbstractCursorCache> extends QueryMethodResult {
    /**
     * 游标ID
     */
    protected String cursorId;

    private T cursorCache;

    public T getCursorCache() {
        return cursorCache;
    }

    public void setCursorCache(T cursorCache) {
        this.cursorCache = cursorCache;
    }

    public String getCursorId() {
        return cursorId;
    }

    public void setCursorId(String cursorId) {
        this.cursorId = cursorId;
    }

    public QueryCursorMethodResult(boolean success, T cursorCache) {
        super(success);
        this.cursorCache = cursorCache;
    }

    public QueryCursorMethodResult(T cursorCache) {
        this.cursorCache = cursorCache;
    }

    public QueryCursorMethodResult() {
        super();
    }

    public QueryCursorMethodResult(long total, T cursorCache) {
        super(total, Collections.emptyList());
        this.cursorCache = cursorCache;
    }
}
