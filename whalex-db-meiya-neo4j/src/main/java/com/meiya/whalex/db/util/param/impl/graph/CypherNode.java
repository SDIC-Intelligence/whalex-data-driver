package com.meiya.whalex.db.util.param.impl.graph;

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
public class CypherNode extends CypherObject<CypherRelationship, CypherRelationship> {

    /**
     * 回调函数
     */
    private Consumer<CypherNode> consumer;

    public boolean hasNextRelationship() {
        return super.hasNextStep();
    }

    public boolean hasPreviousRelationship() {
        return super.hasPreviousStep();
    }

    @Builder(toBuilder = true)
    public CypherNode(int currentIndex, List<String> label, CypherRelationship previousStep, CypherRelationship nextStep, Consumer<CypherNode> consumer, Boolean hidden) {
        super(label, currentIndex, previousStep, nextStep, hidden);
        if (consumer != null) {
            this.consumer = consumer;
            this.consumer.accept(this);
        }
    }

    public CypherRelationship.CypherRelationshipBuilder createNextRelationship() {
        int currentIndex = 1;
        if (previousStep != null) {
            currentIndex = previousStep.getCurrentIndex() + 1;
        }
        return CypherRelationship.builder().currentIndex(currentIndex).previousStep(this).consumer(new Consumer<CypherRelationship>() {
            @Override
            public void accept(CypherRelationship cypherRelationship) {
                CypherNode.this.nextStep = cypherRelationship;
            }
        });
    }
}
