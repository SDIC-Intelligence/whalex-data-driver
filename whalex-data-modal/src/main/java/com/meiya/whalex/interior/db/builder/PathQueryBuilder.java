package com.meiya.whalex.interior.db.builder;

import cn.hutool.core.collection.CollectionUtil;
import com.fasterxml.jackson.annotation.JsonValue;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.Where;

/**
 * @author 黄河森
 * @date 2023/4/23
 * @package com.meiya.whalex.interior.db.builder
 * @project whalex-data-driver
 */
public class PathQueryBuilder {

    private Where where;

    PathQueryBuilder(Where where) {
        this.where = where;
    }

    public Where build() {
        return this.where;
    }

    public PathQueryBuilder startVertex(Where where) {
        Where startWhere = Where.create(GraphQueryBuilder.START_VERTEX, Rel.AND);
        startWhere.setParams(CollectionUtil.newArrayList(where));
        this.where.getParams().add(startWhere);
        return this;
    }

    public PathQueryBuilder endVertex(Where where) {
        Where endWhere = Where.create(GraphQueryBuilder.END_VERTEX, Rel.AND);
        endWhere.setParams(CollectionUtil.newArrayList(where));
        this.where.getParams().add(endWhere);
        return this;
    }

    public PathQueryBuilder edge(Where where) {
        Where edgeWhere = Where.create(GraphQueryBuilder.PATH_EDGE, Rel.AND);
        edgeWhere.setParams(CollectionUtil.newArrayList(where));
        this.where.getParams().add(edgeWhere);
        return this;
    }

    public PathQueryBuilder layer(int layer) {
        Where layerWhere = Where.create(GraphQueryBuilder.LAYER, layer, Rel.EQ);
        this.where.getParams().add(layerWhere);
        return this;
    }

    public PathQueryBuilder option(PathOption option) {
        Where layerWhere = Where.create(GraphQueryBuilder.OPTION, option, Rel.EQ);
        this.where.getParams().add(layerWhere);
        return this;
    }

    public enum PathOption {
        ALL("all"),
        SIMPLE("simple"),
        CYCLIC("cyclic")
        ;

        PathOption(String option) {
            this.option = option;
        }

        private String option;

        @JsonValue
        public String getOption() {
            return option;
        }
    }
}
