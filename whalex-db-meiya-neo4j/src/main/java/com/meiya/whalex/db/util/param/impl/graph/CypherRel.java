package com.meiya.whalex.db.util.param.impl.graph;

/**
 * @author 黄河森
 * @date 2023/3/22
 * @package com.meiya.whalex.db.util.param.impl.graph
 * @project whalex-data-driver
 */
public enum CypherRel {

    AND("AND"),
    OR("OR"),
    NOT_NULL("IS NOT NULL"),
    IS_NULL("IS NULL"),
    XOR("XOR"),
    EQ("="),
    NE("<>"),
    LT("<"),
    LTE("<="),
    GT(">"),
    GTE(">="),
    IN("IN")
    ;

    private String type;

    CypherRel(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
