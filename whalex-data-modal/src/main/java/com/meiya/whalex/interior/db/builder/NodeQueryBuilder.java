package com.meiya.whalex.interior.db.builder;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.Where;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 黄河森
 * @date 2023/4/23
 * @package com.meiya.whalex.interior.db.builder
 * @project whalex-data-driver
 */
public class NodeQueryBuilder {

    private Where where;

    NodeQueryBuilder(Where where) {
        this.where = where;
    }

    public Where build() {
        return this.where;
    }

    public NodeQueryBuilder eq(String field, Object param) {
        return operation(field, param, Rel.EQ);
    }

    public NodeQueryBuilder ne(String field, Object param) {
        return operation(field, param, Rel.NE);
    }

    public NodeQueryBuilder gt(String field, Object param) {
        return operation(field, param, Rel.GT);
    }

    public NodeQueryBuilder gte(String field, Object param) {
        return operation(field, param, Rel.GTE);
    }

    public NodeQueryBuilder lte(String field, Object param) {
        return operation(field, param, Rel.LTE);
    }

    public NodeQueryBuilder.InnerRangeQueryBuilder range(String field) {
        Where where = Where.create();
        where.setField(field);
        where.setType(Rel.BETWEEN);
        operation(where);
        return NodeQueryBuilder.InnerRangeQueryBuilder.builder(where, this);
    }

    public NodeQueryBuilder in(String field, Object... params) {
        return operation(field, CollectionUtil.newArrayList(params), Rel.IN);
    }

    public NodeQueryBuilder notIn(String field, Object... params) {
        return operation(field, CollectionUtil.newArrayList(params), Rel.NIN);
    }

    public NodeQueryBuilder isNull(String field) {
        operation(field, null, Rel.NULL);
        return this;
    }

    public NodeQueryBuilder notNull(String field) {
        operation(field, null, Rel.NOT_NULL);
        return this;
    }

    public NodeQueryBuilder exists(String field) {
        operation(field, null, Rel.EXISTS);
        return this;
    }

    public NodeQueryBuilder like(String field, String param) {
        operation(field, param, Rel.MIDDLE_LIKE);
        return this;
    }

    public NodeQueryBuilder notLike(String field, String param) {
        operation(field, param, Rel.NOT_MIDDLE_LIKE);
        return this;
    }

    public NodeQueryBuilder operation(String field, Object param, Rel rel) {
        Where where = Where.create(field, param, rel);
        return operation(where);
    }

    public NodeQueryBuilder operation(Where where) {
        Rel type = this.where.getType();
        if (!Rel.AND.equals(type)) {
            Where andWhere = Where.create();
            List<Where> wheres = new ArrayList<>();
            andWhere.setParams(wheres);
            wheres.add(this.where);
            this.where = andWhere;
        }
        List<Where> params = this.where.getParams();
        if (params == null) {
            params = new ArrayList<>();
            this.where.setParams(params);
        }
        params.add(where);
        return this;
    }

    /**
     * 内部范围查询
     */
    public static class InnerRangeQueryBuilder {
        private Where where;

        private NodeQueryBuilder current;

        private static Integer MAX_INDEX = 2;

        private InnerRangeQueryBuilder(Where where, NodeQueryBuilder builder) {
            this.current = builder;
            this.where = where;
        }

        public static NodeQueryBuilder.InnerRangeQueryBuilder builder(Where where, NodeQueryBuilder builder) {
            where.setParams(Where.createParams());
            return new NodeQueryBuilder.InnerRangeQueryBuilder(where, builder);
        }

        public NodeQueryBuilder returned() {
            if (this.where.getParams().size() < 2 || this.where.getParams().size() > 2) {
                throw new IllegalArgumentException("范围检索必须设置有且仅有两个最大最小区间值!");
            }
            return current;
        }

        public NodeQueryBuilder.InnerRangeQueryBuilder gt(Object param) {
            return operation(param, Rel.GT);
        }

        public NodeQueryBuilder.InnerRangeQueryBuilder gte(Object param) {
            return operation(param, Rel.GTE);
        }

        public NodeQueryBuilder.InnerRangeQueryBuilder lt(Object param) {
            return operation(param, Rel.LT);
        }

        public NodeQueryBuilder.InnerRangeQueryBuilder lte(Object param) {
            return operation(param, Rel.LTE);
        }

        public NodeQueryBuilder.InnerRangeQueryBuilder operation(Object param, Rel rel) {
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
            Where where = Where.create(this.where.getField(), param, rel);
            params.add(where);
            return this;
        }
    }
}
