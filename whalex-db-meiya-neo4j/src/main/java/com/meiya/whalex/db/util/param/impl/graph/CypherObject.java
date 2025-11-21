package com.meiya.whalex.db.util.param.impl.graph;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author 黄河森
 * @date 2023/3/22
 * @package com.meiya.whalex.db.util.param.impl.graph
 * @project whalex-data-driver
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CypherObject<PREVIOUS extends CypherObject, NEXT extends CypherObject> {
    public String as;

    public List<String> label;

    public int currentIndex = 1;

    public PREVIOUS previousStep;

    public NEXT nextStep;

    /**
     * 如果为 true 则表示是在 gremlin 转 cql 过程中需要的临时性添加节点
     * 比如 g.V().both()  -> MATCH (n1) - [r1] - (n1), 这时候 r1 就是转换过程中添加的临时性节点
     * 在 return 中 不能含有  hidden 节点 内容
     */
    public boolean hidden = false;

    public boolean hasNextStep() {
        return nextStep != null;
    }

    public boolean hasPreviousStep() {
        return previousStep != null;
    }

    public CypherObject(List<String> label, int currentIndex, PREVIOUS previousStep, NEXT nextStep, Boolean hidden) {
        this.currentIndex = currentIndex;
        if (hidden != null) {
            this.hidden = hidden;
        }
        if (!this.hidden) {
            if (this instanceof CypherRelationship) {
                this.as = "r" + currentIndex;
            } else {
                this.as = "n" + currentIndex;
            }
        } else {
            this.as = "";
        }
        this.label = label;
        this.previousStep = previousStep;
        this.nextStep = nextStep;
    }
}
