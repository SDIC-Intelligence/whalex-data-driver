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
public class NestWhereBuilder {

    Where where;

    NestWhereBuilder(Rel rel) {
        this.where = Where.create();
        this.where.setType(rel);
        this.where.setParams(Where.createParams());
    }

    public Where build() {
        return this.where;
    }

    public NestWhereBuilder eq(String field, Object param) {
        return operation(field, param, Rel.EQ);
    }

    public NestWhereBuilder eq(String field, Object param, int boost) {
        return operation(field, param, Rel.EQ, null, boost);
    }

    public NestWhereBuilder eq(String field, Object param, String format) {
        operation(field, param, Rel.EQ, format);
        return this;
    }

    public NestWhereBuilder eq(String field, Object param, ItemFieldTypeEnum fieldType) {
        operation(field, param, fieldType, Rel.EQ);
        return this;
    }

    public NestWhereBuilder eq(String field, Object param, ItemFieldTypeEnum fieldType, String format) {
        operation(field, param, fieldType, Rel.EQ, format);
        return this;
    }

    public NestWhereBuilder eq(String field, Object param, ItemFieldTypeEnum fieldType, String format, int boost) {
        operation(field, param, fieldType, Rel.EQ, format, boost);
        return this;
    }

    public NestWhereBuilder ne(String field, Object param) {
        return operation(field, param, Rel.NE);
    }

    public NestWhereBuilder ne(String field, Object param, int boost) {
        return ne(field, param, null, boost);
    }

    public NestWhereBuilder ne(String field, Object param, String format) {
        operation(field, param, Rel.NE, format);
        return this;
    }

    public NestWhereBuilder ne(String field, Object param, ItemFieldTypeEnum fieldType) {
        operation(field, param, fieldType, Rel.NE);
        return this;
    }

    public NestWhereBuilder ne(String field, Object param, ItemFieldTypeEnum fieldType, String format) {
        operation(field, param, fieldType, Rel.NE, format);
        return this;
    }

    public NestWhereBuilder ne(String field, Object param, String format, int boost) {
        operation(field, param, Rel.NE, format, boost);
        return this;
    }

    public NestWhereBuilder ne(String field, Object param, ItemFieldTypeEnum fieldType, String format, int boost) {
        operation(field, param, fieldType, Rel.NE, format, boost);
        return this;
    }

    public NestWhereBuilder gt(String field, Object param) {
        return operation(field, param, Rel.GT);
    }

    public NestWhereBuilder gt(String field, Object param, int boost) {
        return operation(field, param, Rel.GT, null, boost);
    }

    public NestWhereBuilder gt(String field, Object param, String format) {
        operation(field, param, Rel.GT, format);
        return this;
    }

    public NestWhereBuilder gt(String field, Object param, ItemFieldTypeEnum fieldType) {
        operation(field, param, fieldType, Rel.GT);
        return this;
    }

    public NestWhereBuilder gt(String field, Object param, String format, int boost) {
        operation(field, param, Rel.GT, format, boost);
        return this;
    }

    public NestWhereBuilder gt(String field, Object param, ItemFieldTypeEnum fieldType, String format) {
        operation(field, param, fieldType, Rel.GT, format);
        return this;
    }

    public NestWhereBuilder gt(String field, Object param, ItemFieldTypeEnum fieldType, String format, int boost) {
        operation(field, param, fieldType, Rel.GT, format, boost);
        return this;
    }

    public NestWhereBuilder gte(String field, Object param) {
        return operation(field, param, Rel.GTE);
    }

    public NestWhereBuilder gte(String field, Object param, int boost) {
        return operation(field, param, Rel.GTE, null, boost);
    }

    public NestWhereBuilder gte(String field, Object param, String format) {
        operation(field, param, Rel.GTE, format);
        return this;
    }

    public NestWhereBuilder gte(String field, Object param, ItemFieldTypeEnum fieldType, String format) {
        operation(field, param, fieldType, Rel.GTE, format);
        return this;
    }

    public NestWhereBuilder gte(String field, Object param, String format, int boost) {
        operation(field, param, Rel.GTE, format, boost);
        return this;
    }

    public NestWhereBuilder gte(String field, Object param, ItemFieldTypeEnum fieldType, String format, int boost) {
        operation(field, param, fieldType, Rel.GTE, format, boost);
        return this;
    }

    public NestWhereBuilder lt(String field, Object param) {
        return operation(field, param, Rel.LT);
    }

    public NestWhereBuilder lt(String field, Object param, int boost) {
        return operation(field, param, Rel.LT, null, boost);
    }

    public NestWhereBuilder lt(String field, Object param, String format) {
        operation(field, param, Rel.LT, format);
        return this;
    }

    public NestWhereBuilder lt(String field, Object param, ItemFieldTypeEnum fieldType) {
        operation(field, param, fieldType, Rel.LT);
        return this;
    }

    public NestWhereBuilder lt(String field, Object param, String format, int boost) {
        operation(field, param, Rel.LT, format, boost);
        return this;
    }

    public NestWhereBuilder lt(String field, Object param, ItemFieldTypeEnum fieldType, String format, int boost) {
        operation(field, param, fieldType, Rel.LT, format, boost);
        return this;
    }

    public NestWhereBuilder lte(String field, Object param) {
        return operation(field, param, Rel.LTE);
    }

    public NestWhereBuilder lte(String field, Object param, int boost) {
        return operation(field, param, Rel.LTE, null, boost);
    }

    public NestWhereBuilder lte(String field, Object param, String format) {
        operation(field, param, Rel.LTE, format);
        return this;
    }

    public NestWhereBuilder lte(String field, Object param, ItemFieldTypeEnum fieldType) {
        operation(field, param, fieldType, Rel.LTE);
        return this;
    }

    public NestWhereBuilder lte(String field, Object param, String format, int boost) {
        operation(field, param, Rel.LTE, format, boost);
        return this;
    }

    public NestWhereBuilder lte(String field, Object param, ItemFieldTypeEnum fieldType, String format, int boost) {
        operation(field, param, fieldType, Rel.LTE, format, boost);
        return this;
    }

    public InnerRangeQueryBuilder range(String field) {
        Where where = Where.create();
        where.setField(field);
        this.where.getParams().add(where);
        return InnerRangeQueryBuilder.builder(where, this);
    }

    public InnerRangeQueryBuilder range(String field, int boost) {
        Where where = Where.create();
        where.setBoost(boost);
        where.setField(field);
        this.where.getParams().add(where);
        return InnerRangeQueryBuilder.builder(where, this);
    }

    public InnerInWhereBuilder in(String field, String format, int boost) {
        Where where = Where.create();
        where.setField(field);
        where.setType(Rel.IN);
        where.setBoost(boost);
        where.setParamFormat(format);
        this.where.getParams().add(where);
        return InnerInWhereBuilder.builder(where, this);
    }

    public InnerInWhereBuilder in(String field, String format) {
        return in(field, format, 1);
    }

    public InnerInWhereBuilder in(String field) {
        return in(field, null, 1);
    }

    public InnerInWhereBuilder notIn(String field, String format, int boost) {
        Where where = Where.create();
        where.setField(field);
        where.setType(Rel.NIN);
        where.setBoost(boost);
        where.setParamFormat(format);
        this.where.getParams().add(where);
        return InnerInWhereBuilder.builder(where, this);
    }

    public InnerInWhereBuilder notIn(String field, String format) {
        return notIn(field, format, 1);
    }

    public InnerInWhereBuilder notIn(String field) {
        return notIn(field, null, 1);
    }

    public NestWhereBuilder isNull(String field) {
        operation(field, null, Rel.NULL, null);
        return this;
    }

    public NestWhereBuilder isNull(String field, int boost) {
        operation(field, null, Rel.NULL, null, boost);
        return this;
    }

    public NestWhereBuilder notNull(String field) {
        operation(field, null, Rel.NOT_NULL, null);
        return this;
    }

    public NestWhereBuilder notNull(String field, int boost) {
        operation(field, null, Rel.NOT_NULL, null, boost);
        return this;
    }

    public NestWhereBuilder exists(String field) {
        operation(field, null, Rel.EXISTS, null);
        return this;
    }

    public NestWhereBuilder exists(String field, int boost) {
        operation(field, null, Rel.EXISTS, null, boost);
        return this;
    }

    public NestWhereBuilder term(String field, Object param) {
        return term(field, param, null);
    }

    public NestWhereBuilder term(String field, Object param, int boost) {
        return term(field, param, null, boost);
    }

    public NestWhereBuilder term(String field, Object param, String format) {
        operation(field, param, Rel.TERM, null);
        return this;
    }

    public NestWhereBuilder term(String field, Object param, String format, int boost) {
        operation(field, param, Rel.TERM, null, boost);
        return this;
    }

    public NestWhereBuilder matchAll(String field, Object param, String format, int boost) {
        operation(field, param, Rel.MATCH_ALL, null, boost);
        return this;
    }

    public NestWhereBuilder matchAll(String field, Object param) {
        return matchAll(field, param, null);
    }

    public NestWhereBuilder matchAll(String field, Object param, int boost) {
        return matchAll(field, param, null, boost);
    }

    public NestWhereBuilder matchAll(String field, Object param, String format) {
        operation(field, param, Rel.MATCH_ALL, null);
        return this;
    }

    public NestWhereBuilder matchPhrase(String field, Object param, String format, int boost) {
        operation(field, param, Rel.MATCH_PHRASE, null, boost);
        return this;
    }

    public NestWhereBuilder matchPhrase(String field, Object param) {
        return matchPhrase(field, param, null);
    }

    public NestWhereBuilder matchPhrase(String field, Object param, int boost) {
        return matchPhrase(field, param, null, boost);
    }

    public NestWhereBuilder matchPhrase(String field, Object param, String format) {
        operation(field, param, Rel.MATCH_PHRASE, null);
        return this;
    }

    public NestWhereBuilder regexp(String field, Object param) {
        return regexp(field, param, null);
    }

    public NestWhereBuilder regexp(String field, Object param, String format) {
        operation(field, param, Rel.REGEX, null);
        return this;
    }

    public NestWhereBuilder regexp(String field, Object param, int boost) {
        return regexp(field, param, null, boost);
    }


    public NestWhereBuilder regexp(String field, Object param, String format, int boost) {
        operation(field, param, Rel.REGEX, null, boost);
        return this;
    }

    public NestWhereBuilder multiMatch(String field, Object param, String queryType, String operator) {
        return operation(field, param, Rel.MULTI_MATCH, queryType, operator);
    }

    public NestWhereBuilder match(String field, Object param) {
        return match(field, param, null);
    }

    public NestWhereBuilder match(String field, Object param, int boost) {
        return match(field, param, null, boost);
    }

    public NestWhereBuilder match(String field, Object param, String format) {
        operation(field, param, Rel.MATCH, null);
        return this;
    }

    public NestWhereBuilder match(String field, Object param, String format, int boost) {
        operation(field, param, Rel.MATCH, null, boost);
        return this;
    }

    public NestWhereBuilder ilike(String field, String param) {
        return operation(field, param, Rel.ILIKE);
    }

    public NestWhereBuilder ilike(String field, String param, String format) {
        operation(field, param, Rel.ILIKE, format);
        return this;
    }

    public NestWhereBuilder ilike(String field, String param, ItemFieldTypeEnum fieldType) {
        operation(field, param, fieldType, Rel.ILIKE);
        return this;
    }

    public NestWhereBuilder ilike(String field, String param, int boost) {
        return ilike(field, param, null, boost);
    }

    public NestWhereBuilder ilike(String field, String param, String format, int boost) {
        operation(field, param, Rel.ILIKE, format, boost);
        return this;
    }

    public NestWhereBuilder ilike(String field, String param, ItemFieldTypeEnum fieldType, String format, int boost) {
        operation(field, param, fieldType, Rel.ILIKE, format, boost);
        return this;
    }

    public NestWhereBuilder like(String field, String param) {
        return operation(field, param, Rel.LIKE);
    }

    public NestWhereBuilder like(String field, String param, int boost) {
        return operation(field, param, Rel.LIKE, null, boost);
    }

    public NestWhereBuilder like(String field, String param, String format) {
        operation(field, param, Rel.MIDDLE_LIKE, format);
        return this;
    }

    public NestWhereBuilder like(String field, String param, ItemFieldTypeEnum fieldType) {
        operation(field, param, fieldType, Rel.MIDDLE_LIKE);
        return this;
    }

    public NestWhereBuilder like(String field, String param, String format, int boost) {
        operation(field, param, Rel.MIDDLE_LIKE, format, boost);
        return this;
    }

    public NestWhereBuilder like(String field, String param, ItemFieldTypeEnum fieldType, String format, int boost) {
        operation(field, param, fieldType, Rel.MIDDLE_LIKE, format, boost);
        return this;
    }

    public NestWhereBuilder notLike(String field, String param) {
        return operation(field, param, Rel.NOT_MIDDLE_LIKE);
    }

    public NestWhereBuilder notLike(String field, String param, int  boost) {
        return operation(field, param, Rel.NOT_MIDDLE_LIKE, null, boost);
    }

    public NestWhereBuilder notLike(String field, String param, String format) {
        operation(field, param, Rel.NOT_MIDDLE_LIKE, format);
        return this;
    }

    public NestWhereBuilder notLike(String field, String param, ItemFieldTypeEnum fieldTypeEnum) {
        operation(field, param, fieldTypeEnum, Rel.NOT_MIDDLE_LIKE);
        return this;
    }

    public NestWhereBuilder notLike(String field, String param, String format, int boost) {
        operation(field, param, Rel.NOT_MIDDLE_LIKE, format, boost);
        return this;
    }

    public NestWhereBuilder notLike(String field, String param, ItemFieldTypeEnum fieldType, String format) {
        operation(field, param, fieldType, Rel.NOT_MIDDLE_LIKE, format);
        return this;
    }

    public NestWhereBuilder notLike(String field, String param, ItemFieldTypeEnum fieldType, String format, int boost) {
        operation(field, param, fieldType, Rel.NOT_MIDDLE_LIKE, format, boost);
        return this;
    }

    public NestWhereBuilder radius(String field, long distance, DistanceUnitEnum unit, double lon, double lat) {
        if (unit == null) {
            unit = DistanceUnitEnum.KILOMETERS;
        }
        Map<String, String> param = MapUtil.builder("distance", distance + unit.getUnit())
                .put(CommonConstant.LOC, lon + "," + lat).build();
        operation(field, param, Rel.GEO_DISTANCE, null);
        return this;
    }

    public NestWhereBuilder radius(String field, long distance, DistanceUnitEnum unit, double lon, double lat, int boost) {
        if (unit == null) {
            unit = DistanceUnitEnum.KILOMETERS;
        }
        Map<String, String> param = MapUtil.builder("distance", distance + unit.getUnit())
                .put(CommonConstant.LOC, lon + "," + lat).build();
        operation(field, param, Rel.GEO_DISTANCE, null, boost);
        return this;
    }

    public NestWhereBuilder box(String field, Point topLeft, Point bottomRight) {
        Map<String, String> param = MapUtil.builder("topLeft", topLeft.getLon() + "," + topLeft.getLat())
                .put("bottomRight", bottomRight.getLon() + "," + bottomRight.getLat()).build();
        operation(field, param, Rel.GEO_BOX, null);
        return this;
    }

    public NestWhereBuilder box(String field, Point topLeft, Point bottomRight, int boost) {
        Map<String, String> param = MapUtil.builder("topLeft", topLeft.getLon() + "," + topLeft.getLat())
                .put("bottomRight", bottomRight.getLon() + "," + bottomRight.getLat()).build();
        operation(field, param, Rel.GEO_BOX, null, boost);
        return this;
    }

    public NestWhereBuilder polygon(String field, Point... points) {
        List<Map<String, String>> param = new ArrayList<>(points.length);
        for (Point point : points) {
            param.add(MapUtil.builder("lat", String.valueOf(point.getLat())).put("lon", String.valueOf(point.getLon())).build());
        }
        operation(field, param, Rel.GEO_POLYGON, null);
        return this;
    }

    public NestWhereBuilder polygon(String field, int boost, Point... points) {
        List<Map<String, String>> param = new ArrayList<>(points.length);
        for (Point point : points) {
            param.add(MapUtil.builder("lat", String.valueOf(point.getLat())).put("lon", String.valueOf(point.getLon())).build());
        }
        operation(field, param, Rel.GEO_POLYGON, null, boost);
        return this;
    }

    public NestWhereBuilder operation(String field, Object param, Rel rel) {
        this.where.addWhere(field, param, rel);
        return this;
    }

    public NestWhereBuilder operation(String field, Object param, ItemFieldTypeEnum fieldType, Rel rel) {
        this.where.addWhere(field, param, fieldType.getVal(), rel);
        return this;
    }

    public NestWhereBuilder operation(String field, Object param, ItemFieldTypeEnum fieldType, Rel rel, String format) {
        this.where.addWhere(field, param, fieldType.getVal(), rel, format);
        return this;
    }

    public NestWhereBuilder operation(String field, Object param, Rel rel, String format) {
        this.where.addWhere(field, param, rel, format);
        return this;
    }

    public NestWhereBuilder operation(String field, Object param, Rel rel, String queryType, String operator) {
        this.where.addWhere(field, param, rel, queryType, operator);
        return this;
    }

    public NestWhereBuilder operation(String field, Object param, Rel rel, String format, int boost) {
        this.where.addWhere(field, param, rel, format, boost);
        return this;
    }

    public NestWhereBuilder operation(String field, Object param, ItemFieldTypeEnum fieldType, Rel rel, String format, int boost) {
        this.where.addWhere(field, param, fieldType.getVal(), rel, format, boost);
        return this;
    }

    public InnerNestedWhereBuilder nested(String field) {
        Where where = Where.create();
        where.setField(field);
        where.setType(Rel.NESTED);
        this.where.getParams().add(where);
        return InnerNestedWhereBuilder.builder(where, this);
    }

    public InnerNestedWhereBuilder nested(String field, int boost) {
        Where where = Where.create();
        where.setField(field);
        where.setBoost(boost);
        where.setType(Rel.NESTED);
        this.where.getParams().add(where);
        return InnerNestedWhereBuilder.builder(where, this);
    }

    /**
     * 内部范围查询
     */
    public static class InnerRangeQueryBuilder {
        private Where where;

        private NestWhereBuilder current;

        private static Integer MAX_INDEX = 2;

        private InnerRangeQueryBuilder(Where where, NestWhereBuilder builder) {
            this.current = builder;
            this.where = where;
        }

        public static InnerRangeQueryBuilder builder(Where where, NestWhereBuilder builder) {
            where.setParams(Where.createParams());
            return new InnerRangeQueryBuilder(where, builder);
        }

        public NestWhereBuilder returned() {
            if (this.where.getParams().size() < 2 || this.where.getParams().size() > 2) {
                throw new IllegalArgumentException("范围检索必须设置有且仅有两个最大最小区间值!");
            }
            return current;
        }

        public InnerRangeQueryBuilder gt(Object param) {
            return operation(param, Rel.GT, null);
        }

        public InnerRangeQueryBuilder gt(Object param, String format) {
            return operation(param, Rel.GT, format);
        }

        public InnerRangeQueryBuilder gte(Object param) {
            return operation(param, Rel.GTE, null);
        }

        public InnerRangeQueryBuilder gte(Object param, String format) {
            return operation(param, Rel.GTE, format);
        }

        public InnerRangeQueryBuilder lt(Object param) {
            return operation(param, Rel.LT, null);
        }

        public InnerRangeQueryBuilder lt(Object param, String format) {
            return operation(param, Rel.LT, format);
        }

        public InnerRangeQueryBuilder lte(Object param) {
            return operation(param, Rel.LTE, null);
        }

        public InnerRangeQueryBuilder lte(Object param, String format) {
            return operation(param, Rel.LTE, format);
        }

        public InnerRangeQueryBuilder operation(Object param, Rel rel, String format) {
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
            Where where = Where.create(this.where.getField(), param, rel, format);
            where.setBoost(this.where.getBoost());
            params.add(where);
            return this;
        }
    }

    /**
     * nested 操作
     */
    public static class InnerNestedWhereBuilder {
        private Where where;
        private NestWhereBuilder current;

        private InnerNestedWhereBuilder(Where where, NestWhereBuilder current) {
            this.where = where;
            this.where.setParams(Where.createParams());
            this.current = current;
        }

        private static InnerNestedWhereBuilder builder(Where where, NestWhereBuilder current) {
            return new InnerNestedWhereBuilder(where, current);
        }

        public InnerNestedWhereBuilder nestedWhere(Where where) {
            this.where.getParams().add(where);
            return this;
        }

        public NestWhereBuilder returned() {
            return this.current;
        }
    }

    public static class InnerInWhereBuilder {
        private Where where;
        private NestWhereBuilder current;

        private InnerInWhereBuilder(Where where, NestWhereBuilder current) {
            this.where = where;
            this.current = current;
        }

        private static InnerInWhereBuilder builder(Where where, NestWhereBuilder current) {
            return new InnerInWhereBuilder(where, current);
        }

        public InnerInWhereBuilder values(List params) {
            this.where.setParam(params);
            return this;
        }

        public InnerInWhereBuilder values(Object... params) {
            this.where.setParam(CollectionUtil.newArrayList(params));
            return this;
        }

        public NestWhereBuilder returned() {
            return this.current;
        }
    }
    
}
