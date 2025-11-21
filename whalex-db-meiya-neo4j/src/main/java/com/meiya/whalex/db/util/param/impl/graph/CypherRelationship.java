package com.meiya.whalex.db.util.param.impl.graph;

import com.meiya.whalex.graph.entity.GraphDirection;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author 黄河森
 * @date 2023/3/22
 * @package com.meiya.whalex.db.util.param.impl.graph
 * @project whalex-data-driver
 */
@Data
@NoArgsConstructor
public class CypherRelationship extends CypherObject<CypherNode, CypherNode> {

    private GraphDirection direction;

    /**
     * 开始层级
     */
    private Integer start;

    /**
     * 结束层级
     */
    private Integer end;

    /**
     * 回调函数
     */
    private Consumer<CypherRelationship> consumer;

    public boolean hasNextNode() {
        return super.hasNextStep();
    }

    public boolean hasPreviousNode() {
        return super.hasPreviousStep();
    }

    @Builder(toBuilder = true)
    public CypherRelationship(int currentIndex, List<String> label, GraphDirection direction, CypherNode nextStep, CypherNode previousStep, Integer start, Integer end, Consumer<CypherRelationship> consumer, Boolean hidden) {
        super(label, currentIndex, previousStep, nextStep, hidden);
        this.direction = direction;
        this.start = start;
        this.end = end;
        if (consumer != null) {
            this.consumer = consumer;
            this.consumer.accept(this);
        }
    }

    public CypherNode.CypherNodeBuilder createNextNode() {
        CypherNode previousNode = this.getPreviousStep();
        int currentIndex = 1;
        if (previousNode != null) {
            currentIndex = previousNode.getCurrentIndex() + 1;
        }
        return CypherNode.builder().previousStep(this).currentIndex(currentIndex).consumer(new Consumer<CypherNode>() {
            @Override
            public void accept(CypherNode cypherNode) {
                CypherRelationship.this.setNextStep(cypherNode);
            }
        });
    }
}
