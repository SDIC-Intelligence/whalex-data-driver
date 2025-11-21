package com.meiya.whalex.interior.db.builder;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.interior.db.constant.CommonConstant;
import com.meiya.whalex.interior.db.constant.DistanceUnitEnum;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.meiya.whalex.interior.db.operation.in.Point;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.Where;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author 黄河森
 * @date 2021/6/18
 * @project whalex-data-driver-back
 */
public class SingleWhereBuilder {
    
    private Where where;

    private SingleWhereBuilder() {
        this.where = Where.create();
    }
    
    public static SingleWhereBuilder builder() {
        return new SingleWhereBuilder();
    }

    public InnerSingleWhereBuilder eq(String field, Object param) {
        operation(field, param, Rel.EQ);
        return InnerSingleWhereBuilder.builder(this.where);
    }

    public InnerSingleWhereBuilder ne(String field, Object param) {
        return operation(field, param, Rel.NE);
    }

    public InnerSingleWhereBuilder gt(String field, Object param) {
        return operation(field, param, Rel.GT);
    }

    public InnerSingleWhereBuilder gte(String field, Object param) {
        return operation(field, param, Rel.GTE);
    }

    public InnerSingleWhereBuilder lt(String field, Object param) {
        return operation(field, param, Rel.LT);
    }

    public InnerSingleWhereBuilder lte(String field, Object param) {
        return operation(field, param, Rel.LTE);
    }

    public InnerRangeQueryBuilder range(String field) {
        this.where.setField(field);
        return InnerRangeQueryBuilder.builder(this.where);
    }

    public InnerSingleWhereBuilder multiMatch(String field, Object param, String queryType, String operator) {
        return operation(field, param, Rel.MULTI_MATCH, queryType, operator);
    }

    public InnerSingleWhereBuilder in(String field, Object... param) {
        return operation(field, CollectionUtil.newArrayList(param), Rel.IN);
    }

    public InnerSingleWhereBuilder notIn(String field, Object... param) {
        return operation(field, CollectionUtil.newArrayList(param), Rel.NIN);
    }

    public InnerSingleWhereBuilder isNull(String field) {
        return operation(field, null, Rel.NULL);
    }

    public InnerSingleWhereBuilder notNull(String field) {
        return operation(field, null, Rel.NOT_NULL);
    }

    public InnerSingleWhereBuilder exists(String field) {
        return operation(field, null, Rel.EXISTS);
    }

    public InnerSingleWhereBuilder term(String field, Object param) {
        return operation(field, param, Rel.TERM);
    }

    public InnerSingleWhereBuilder match(String field, Object param) {
        return operation(field, param, Rel.MATCH);
    }

    public InnerSingleWhereBuilder like(String field, String param) {
        return operation(field, param, Rel.MIDDLE_LIKE);
    }

    public InnerSingleWhereBuilder notLike(String field, String param) {
        return operation(field, param, Rel.NOT_MIDDLE_LIKE);
    }

    public InnerSingleWhereBuilder radius(String field, long distance, DistanceUnitEnum unit, double lon, double lat) {
        if (unit == null) {
            unit = DistanceUnitEnum.KILOMETERS;
        }
        Map<String, String> param = MapUtil.builder("distance", distance + unit.getUnit())
                .put(CommonConstant.LOC, lon + "," + lat).build();
        return operation(field, param, Rel.GEO_DISTANCE);
    }

    public InnerSingleWhereBuilder box(String field, Point topLeft, Point bottomRight) {
        Map<String, String> param = MapUtil.builder("topLeft", topLeft.getLon() + "," + topLeft.getLat())
                .put("bottomRight", bottomRight.getLon() + "," + bottomRight.getLat()).build();
        return operation(field, param, Rel.GEO_BOX);
    }

    public InnerSingleWhereBuilder polygon(String field, Point... points) {
        List<Map<String, String>> param = new ArrayList<>(points.length);
        for (Point point : points) {
            param.add(MapUtil.builder("lat", String.valueOf(point.getLat())).put("lon", String.valueOf(point.getLon())).build());
        }
        return operation(field, param, Rel.GEO_POLYGON);
    }

    public InnerNestedWhereBuilder nested(String field) {
        this.where.setField(field);
        this.where.setType(Rel.NESTED);
        return InnerNestedWhereBuilder.builder(this.where);
    }

    public InnerHasParentWhereBuilder hasParent(String field) {
        this.where.setField(field);
        this.where.setType(Rel.HAS_PARENT);
        return InnerHasParentWhereBuilder.builder(this.where);
    }

    public InnerHasChildWhereBuilder hasChild(String field) {
        this.where.setField(field);
        this.where.setType(Rel.HAS_CHILD);
        return InnerHasChildWhereBuilder.builder(this.where);
    }

    public Where parentId(String field, Object param) {
        this.where.setField(field);
        this.where.setType(Rel.PARENT_ID);
        this.where.setParam(param);
        return this.where;
    }

    public InnerSingleWhereBuilder operation(String field, Object param, Rel rel) {
        this.where.setField(field);
        this.where.setParam(param);
        this.where.setType(rel);
        return InnerSingleWhereBuilder.builder(this.where);
    }

    public InnerSingleWhereBuilder operation(String field, Object param, Rel type, String queryType, String operator) {
        this.where.setField(field);
        this.where.setParam(param);
        this.where.setType(type);
        this.where.setQueryType(queryType);
        this.where.setOperator(operator);
        return InnerSingleWhereBuilder.builder(this.where);
    }

    /**
     * 内部范围查询
     */
    public static class InnerRangeQueryBuilder {
        private Where where;

        private static Integer MAX_INDEX = 2;

        private InnerRangeQueryBuilder(Where where) {
            this.where = where;
        }

        public static InnerRangeQueryBuilder builder(Where where) {
            where.setParams(Where.createParams());
            return new InnerRangeQueryBuilder(where);
        }

        public InnerSingleWhereBuilder returned() {
            if (this.where.getParams().size() < 2 || this.where.getParams().size() > 2) {
                throw new IllegalArgumentException("范围检索必须设置有且仅有两个最大最小区间值!");
            }
            return InnerSingleWhereBuilder.builder(this.where);
        }

        public InnerRangeQueryBuilder gt(Object param) {
            return operation(param, Rel.GT, null);
        }

        public InnerRangeQueryBuilder gte(Object param) {
            return operation(param, Rel.GTE, null);
        }

        public InnerRangeQueryBuilder lt(Object param) {
            return operation(param, Rel.LT, null);
        }

        public InnerRangeQueryBuilder lte(Object param) {
            return operation(param, Rel.LTE, null);
        }

        public InnerRangeQueryBuilder gt(Object param, ItemFieldTypeEnum paramType) {
            return operation(param, Rel.GT, paramType);
        }

        public InnerRangeQueryBuilder gte(Object param, ItemFieldTypeEnum paramType) {
            return operation(param, Rel.GTE, paramType);
        }

        public InnerRangeQueryBuilder lt(Object param, ItemFieldTypeEnum paramType) {
            return operation(param, Rel.LT, paramType);
        }

        public InnerRangeQueryBuilder lte(Object param, ItemFieldTypeEnum paramType) {
            return operation(param, Rel.LTE, paramType);
        }

        public InnerRangeQueryBuilder operation(Object param, Rel rel, ItemFieldTypeEnum paramType) {
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
            String paramTypeVal = null;
            if (paramType != null) {
                paramTypeVal = paramType.getVal();
            }
            params.add(Where.create(this.where.getField(), param, paramTypeVal, rel));
            return this;
        }
    }

    /**
     * 内部 where 查询
     */
    public static class InnerSingleWhereBuilder {
        private Where where;

        private InnerSingleWhereBuilder(Where where) {
            this.where = where;
        }

        private static InnerSingleWhereBuilder builder(Where where) {
            return new InnerSingleWhereBuilder(where);
        }

        public InnerSingleWhereBuilder format(String format) {
            if (CollectionUtil.isEmpty(this.where.getParams())) {
                this.where.setParamFormat(format);
            } else {
                List<Where> params = this.where.getParams();
                for (Where param : params) {
                    param.setParamFormat(format);
                }
            }
            return this;
        }

        public InnerSingleWhereBuilder fieldType(ItemFieldTypeEnum fieldType) {
            if (CollectionUtil.isEmpty(this.where.getParams())) {
                this.where.setParamType(fieldType.getVal());
            } else {
                List<Where> params = this.where.getParams();
                for (Where param : params) {
                    param.setParamType(fieldType.getVal());
                }
            }
            return this;
        }

        public Where build() {
            return this.where;
        }
    }

    /**
     * nested 操作
     */
    public static class InnerNestedWhereBuilder {
        private Where where;

        private InnerNestedWhereBuilder(Where where) {
            this.where = where;
            this.where.setParams(Where.createParams());
        }

        private static InnerNestedWhereBuilder builder(Where where) {
            return new InnerNestedWhereBuilder(where);
        }

        public InnerNestedWhereBuilder nestedWhere(Where where) {
            this.where.getParams().add(where);
            return this;
        }

        public Where build() {
            return this.where;
        }
    }

    /**
     * nested 操作
     */
    public static class InnerHasParentWhereBuilder {
        private Where where;

        private InnerHasParentWhereBuilder(Where where) {
            this.where = where;
            this.where.setParams(Where.createParams());
        }

        private static InnerHasParentWhereBuilder builder(Where where) {
            return new InnerHasParentWhereBuilder(where);
        }

        public InnerHasParentWhereBuilder hasParentWhere(Where where) {
            this.where.getParams().add(where);
            return this;
        }

        public Where build() {
            return this.where;
        }
    }

    /**
     * nested 操作
     */
    public static class InnerHasChildWhereBuilder {
        private Where where;

        private InnerHasChildWhereBuilder(Where where) {
            this.where = where;
            this.where.setParams(Where.createParams());
        }

        private static InnerHasChildWhereBuilder builder(Where where) {
            return new InnerHasChildWhereBuilder(where);
        }

        public InnerHasChildWhereBuilder hasChildWhere(Where where) {
            this.where.getParams().add(where);
            return this;
        }

        public Where build() {
            return this.where;
        }
    }
}
