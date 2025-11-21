package com.meiya.whalex.interior.db.builder;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ArrayUtil;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.Where;

import java.util.ArrayList;

/**
 * @author 黄河森
 * @date 2023/4/23
 * @package com.meiya.whalex.interior.db.builder
 * @project whalex-data-driver
 */
public class GraphQueryBuilder {

    public static final String VERTEX = "_vertex";
    public static final String LABEL = "_label";
    public static final String EDGE = "_edge";
    public static final String PATH = "_path";
    public static final String RETURN = "_return";
    public static final String LAYER = "_layer";
    public static final String OPTION = "_option";
    public static final String START_VERTEX = "_startVertex";
    public static final String END_VERTEX = "_endVertex";
    public static final String PATH_EDGE = "_pathEdge";
    public static final String PATH_DIRECTION = "_pathDirection";
    public static final String PATH_DIRECTION_OUT = "OUT";
    public static final String PATH_DIRECTION_IN = "IN";
    public static final String PATH_DIRECTION_BOTH = "BOTH";

    private Where where;

    private GraphQueryBuilder() {
        this.where = Where.create();
    }

    public static GraphQueryBuilder builder() {
        return new GraphQueryBuilder();
    }

    public NodeQueryBuilder V(String... label) {
        return node(VERTEX, label);
    }

    public NodeQueryBuilder E(String... label) {
        return node(EDGE, label);
    }

    public PathQueryBuilder path() {
        getReturn(PATH);
        return new PathQueryBuilder(where);
    }

    private void getReturn(String type) {
        ArrayList<Where> wheres = new ArrayList<>();
        where.setParams(wheres);
        Where returnWhere = Where.create();
        returnWhere.setField(RETURN);
        returnWhere.setType(Rel.EQ);
        returnWhere.setParam(type);
        wheres.add(returnWhere);
    }

    private NodeQueryBuilder node(String type, String... label) {
        if (ArrayUtil.isNotEmpty(label)) {
            getReturn(type);
            Where labelWhere = Where.create();
            if (label.length > 1) {
                labelWhere.setField(LABEL);
                labelWhere.setType(Rel.IN);
                labelWhere.setParam(CollectionUtil.newArrayList(label));
            } else {
                labelWhere.setField(LABEL);
                labelWhere.setType(Rel.EQ);
                labelWhere.setParam(label[0]);
            }
            this.where.getParams().add(labelWhere);
        } else {
            where.setField(RETURN);
            where.setType(Rel.EQ);
            where.setParam(VERTEX);
        }
        return new NodeQueryBuilder(where);
    }
}
