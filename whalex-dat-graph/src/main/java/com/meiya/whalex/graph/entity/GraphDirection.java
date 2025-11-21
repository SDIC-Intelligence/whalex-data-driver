package com.meiya.whalex.graph.entity;


/**
 * 关系指向
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 */
public enum  GraphDirection {

    OUT,
    IN,
    BOTH,
    /**
     * 兼容 .E() 场景
     * 根据下一个步骤操作决定当前关系指向
     * 比如 .E().outV() 则当前 UNDEFINED -> OUT
     * 用于 Neo4j Gremlin 解析
     */
    UNDEFINED;

    public static final GraphDirection[] proper = new GraphDirection[]{OUT, IN};

    private GraphDirection() {
    }

    public GraphDirection opposite() {
        if (this.equals(OUT)) {
            return IN;
        } else {
            return this.equals(IN) ? OUT : BOTH;
        }
    }

}
