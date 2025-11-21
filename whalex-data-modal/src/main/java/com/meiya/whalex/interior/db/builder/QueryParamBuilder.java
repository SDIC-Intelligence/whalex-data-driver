package com.meiya.whalex.interior.db.builder;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.interior.db.search.in.*;

import java.util.List;

/**
 * @author 黄河森
 * @date 2021/6/18
 * @project whalex-data-driver-back
 */
public class QueryParamBuilder {

    private QueryParamCondition queryParamCondition;

    private QueryParamBuilder() {
        this.queryParamCondition = QueryParamCondition.create();
    }

    public static QueryParamBuilder builder() {
        return new QueryParamBuilder();
    }

    public QueryParamCondition build() {
        return this.queryParamCondition;
    }

    public QueryParamBuilder highlightFields(String... fields) {
        this.queryParamCondition.highlightFields(CollectionUtil.newArrayList(fields));
        return this;
    }

    public QueryParamBuilder preTags(String preTags) {
        this.queryParamCondition.preTags(preTags);
        return this;
    }

    public QueryParamBuilder postTags(String postTags) {
        this.queryParamCondition.postTags(postTags);
        return this;
    }

    public QueryParamBuilder highlightNum(int highlightNum) {
        this.queryParamCondition.highlightNum(highlightNum);
        return this;
    }

    public QueryParamBuilder select(String... fields) {
        this.queryParamCondition.select(CollectionUtil.newArrayList(fields));
        return this;
    }

    public QueryParamBuilder where(Where where) {
        this.queryParamCondition.where(where);
        return this;
    }

    public QueryParamBuilder aggFunction(AggFunction aggFunction) {
        this.queryParamCondition.aggFunction(aggFunction);
        return this;
    }

    public QueryParamBuilder aggregate(Aggs aggs) {
        this.queryParamCondition.aggregate(aggs);
        return this;
    }

    public QueryParamBuilder orders(List<Order> orderList) {
        this.queryParamCondition.order(orderList);
        return this;
    }

    public QueryParamBuilder order(Order order) {
        this.queryParamCondition.order(order);
        return this;
    }

    public QueryParamBuilder asc(String filed) {
        this.queryParamCondition.order(new Order(filed, Sort.ASC));
        return this;
    }

    public QueryParamBuilder asc(String filed, Sort nullSort) {
        this.queryParamCondition.order(new Order(filed, Sort.ASC, nullSort));
        return this;
    }

    public QueryParamBuilder desc(String filed) {
        this.queryParamCondition.order(new Order(filed, Sort.DESC));
        return this;
    }

    public QueryParamBuilder desc(String filed, Sort nullSort) {
        this.queryParamCondition.order(new Order(filed, Sort.DESC, nullSort));
        return this;
    }

    public QueryParamBuilder page(Integer offset, Integer limit) {
        this.queryParamCondition.page(Page.create(offset, limit));
        return this;
    }

    public QueryParamBuilder count() {
        this.queryParamCondition.setCountFlag(true);
        return this;
    }

    public QueryParamBuilder suggest(Suggest suggest) {
        this.queryParamCondition.setSuggest(suggest);
        return this;
    }

    public QueryParamBuilder associated(AssociatedQuery associatedQuery) {
        this.queryParamCondition.setAssociatedQuery(associatedQuery);
        return this;
    }

    public QueryParamBuilder dsl(Object dsl) {
        this.queryParamCondition.setDsl(dsl);
        return this;
    }

    public QueryHintBuilder hint() {
        Hint hint = new Hint();
        this.queryParamCondition.setHint(hint);
        return QueryHintBuilder.builder(this, hint);
    }

    public CursorBuilder cursor() {
        return CursorBuilder.builder(this, this.queryParamCondition);
    }

    /**
     * 游标查询构造
     */
    public static class CursorBuilder {
        private QueryParamCondition queryParamCondition;
        private QueryParamBuilder queryParamBuilder;
        private CursorBuilder(QueryParamBuilder queryParamBuilder, QueryParamCondition queryParamCondition) {
            this.queryParamBuilder = queryParamBuilder;
            this.queryParamCondition = queryParamCondition;
        }
        public static CursorBuilder builder(QueryParamBuilder queryParamBuilder, QueryParamCondition queryParamCondition) {
            return new CursorBuilder(queryParamBuilder, queryParamCondition);
        }
        public QueryParamBuilder returned() {
            return this.queryParamBuilder;
        }
        public CursorBuilder batchSize(Integer batchSize) {
            this.queryParamCondition.setBatchSize(batchSize);
            return this;
        }
        public CursorBuilder cursorId(String cursorId) {
            this.queryParamCondition.setCursorId(cursorId);
            return this;
        }
        public CursorBuilder cursorRollAllData(Boolean cursorRollAllData) {
            this.queryParamCondition.setCursorRollAllData(cursorRollAllData);
            return this;
        }
        public CursorBuilder firstCursor(Boolean firstCursor) {
            this.queryParamCondition.setFirstCursor(firstCursor);
            return this;
        }
    }
}
