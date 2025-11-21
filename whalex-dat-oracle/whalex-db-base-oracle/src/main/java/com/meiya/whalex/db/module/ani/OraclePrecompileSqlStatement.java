package com.meiya.whalex.db.module.ani;

import com.meiya.whalex.sql.module.DefaultPrecompileSqlStatement;

import java.util.ArrayList;
import java.util.List;

public class OraclePrecompileSqlStatement extends DefaultPrecompileSqlStatement {

    private List<OracleSqlParseHandler.LimitOffset> limitOffsets;
    private int dynamicParamAccumulator;

    public OraclePrecompileSqlStatement(String sql, List<OracleSqlParseHandler.LimitOffset> limitOffsets, int dynamicParamAccumulator) {
        super(sql);
        this.limitOffsets = limitOffsets;
        this.dynamicParamAccumulator = dynamicParamAccumulator;
    }

    @Override
    public void paramHandle(List<Object> params) {

        //没有分页，不做处理
        if(limitOffsets.isEmpty()) {
            return;
        }

        //对分页参数进行处理
        List<Object> tempList = new ArrayList<>(dynamicParamAccumulator);

        int index = 0;

        for (OracleSqlParseHandler.LimitOffset limitOffset : limitOffsets) {
            int limitPos = limitOffset.getLimitPos();
            int limitValue = limitOffset.getLimitValue();
            boolean hasLimitValue = limitOffset.isHasLimitValue();
            int offsetPos = limitOffset.getOffsetPos();
            int offsetValue = limitOffset.getOffsetValue();
            boolean hasOffsetValue = limitOffset.isHasOffsetValue();

            while (index < limitPos) {
                tempList.add(params.get(index));
                index++;
            }

            if(!hasLimitValue && !hasOffsetValue) {
                limitValue = Integer.valueOf(params.get(index).toString());
                offsetValue = Integer.valueOf(params.get(index + 1).toString());
            }else if(hasLimitValue && !hasOffsetValue){
                offsetValue = Integer.valueOf(params.get(index).toString());
            }else if(!hasLimitValue && hasOffsetValue){
                limitValue = Integer.valueOf(params.get(index).toString());
            }

            int start = offsetValue + 1;
            int end = offsetValue + limitValue;

            tempList.add(end);
            index++;
            tempList.add(start);
            index++;
        }

        params.clear();
        params.addAll(tempList);
    }
}
