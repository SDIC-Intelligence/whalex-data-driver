package com.meiya.whalex.interior.db.builder;

import com.meiya.whalex.interior.db.search.condition.AggFunctionType;
import com.meiya.whalex.interior.db.search.in.AggFunction;

/**
 * @author 黄河森
 * @date 2021/6/18
 * @project whalex-data-driver-back
 */
public class AggFunctionBuilder {

    private AggFunction aggFunction;

    private AggFunctionBuilder() {
        this.aggFunction = new AggFunction();
    }

    public static AggFunctionBuilder builder() {
        return new AggFunctionBuilder();
    }

    public AggFunctionBuilder sum(String functionName, String field) {
        function(functionName, field, null, AggFunctionType.SUM);
        return this;
    }

    public AggFunctionBuilder avg(String functionName, String field) {
        function(functionName, field, null, AggFunctionType.AVG);
        return this;
    }

    public AggFunctionBuilder max(String functionName, String field) {
        function(functionName, field, null, AggFunctionType.MAX);
        return this;
    }

    public AggFunctionBuilder min(String functionName, String field) {
        function(functionName, field, null, AggFunctionType.MIN);
        return this;
    }

    public AggFunctionBuilder count(String functionName, String field) {
        function(functionName, field, null, AggFunctionType.COUNT);
        return this;
    }

    public AggFunctionBuilder distinct(String functionName, String field) {
        function(functionName, field, null, AggFunctionType.DISTINCT);
        return this;
    }

    public AggFunctionBuilder avgForWeightFieldByEs(String functionName, String field, String weightField) {
        function(functionName, field, weightField, AggFunctionType.AVG);
        return this;
    }

    public AggFunctionBuilder function(String functionName, String field, String weightField, AggFunctionType type) {
        this.aggFunction.setFunctionName(functionName);
        this.aggFunction.setField(field);
        this.aggFunction.setAggFunctionType(type);
        this.aggFunction.setWeightField(weightField);
        return this;
    }

    public AggFunctionBuilder hits(Integer topHit) {
        this.aggFunction.setHitNum(topHit);
        return this;
    }

    public AggFunction build() {
        return aggFunction;
    }
}
