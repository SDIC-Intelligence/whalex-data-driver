package com.meiya.whalex.interior.db.builder;

/**
 * 查询构建工厂
 *
 * @author 黄河森
 * @date 2021/6/18
 * @project whalex-data-driver-back
 */
public class SearchBuilders {

    public static QueryParamBuilder search() {
        return QueryParamBuilder.builder();
    }

    public static AndWhereBuilder and() {
        return AndWhereBuilder.builder();
    }

    public static OrWhereBuilder or() {
        return OrWhereBuilder.builder();
    }

    public static NotWhereBuilder not() {
        return NotWhereBuilder.builder();
    }

    public static SingleWhereBuilder where() {
        return SingleWhereBuilder.builder();
    }

    public static AggregateBuilder aggregate() {
        return AggregateBuilder.builder();
    }

    public static AggFunctionBuilder aggFunction() {
        return AggFunctionBuilder.builder();
    }

    public static SuggestBuilder suggest() {
        return SuggestBuilder.builder();
    }

    public static AssociatedQueryBuilder associated() {
        return AssociatedQueryBuilder.builder();
    }

    public static GraphQueryBuilder graph() {
        return GraphQueryBuilder.builder();
    }
}
