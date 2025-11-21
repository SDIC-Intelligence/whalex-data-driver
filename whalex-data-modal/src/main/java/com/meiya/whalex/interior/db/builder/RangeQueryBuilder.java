package com.meiya.whalex.interior.db.builder;

import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.Where;

import java.util.List;

/**
 * @author 黄河森
 * @date 2021/6/18
 * @project whalex-data-driver-back
 */
public class RangeQueryBuilder {

    private Where where;

    private static Integer MAX_INDEX = 2;

    public RangeQueryBuilder(String field) {
        this.where = Where.create();
        this.where.setType(Rel.AND);
        this.where.setParams(Where.createParams());
        this.where.setField(field);
    }

    public static RangeQueryBuilder builder(String field) {
        return new RangeQueryBuilder(field);
    }

    public Where build() {
        return where;
    }

    public RangeQueryBuilder gt(Object param) {
        return operation(param, Rel.GT);
    }

    public RangeQueryBuilder gte(Object param) {
        return operation(param, Rel.GTE);
    }

    public RangeQueryBuilder lt(Object param) {
        return operation(param, Rel.LT);
    }

    public RangeQueryBuilder lte(Object param) {
        return operation(param, Rel.LTE);
    }

    public RangeQueryBuilder operation(Object param, Rel rel) {
        if (this.where.getParams().size() > MAX_INDEX) {
            throw new IllegalArgumentException("范围检索已明确边界，无法继续增加更多边界值!");
        }
        List<Where> params = this.where.getParams();
        switch (rel) {
            case GT:
            case GTE:
                if (params.stream().filter(k -> k.getType().equals(Rel.GT) || k.getType().equals(Rel.GTE)).count() > 0) {
                    throw new IllegalArgumentException("范围检索已经存在 GT/GTE 边界值，无法再次设置相同边界值!");
                }
                break;
            case LTE:
            case LT:
                if (params.stream().filter(k -> k.getType().equals(Rel.LT) || k.getType().equals(Rel.LTE)).count() > 0) {
                    throw new IllegalArgumentException("范围检索已经存在 LT/LTE 边界值，无法再次设置相同边界值!");
                }
                break;
        }
        params.add(Where.create(this.where.getField(), param, rel));
        return this;
    }
}
