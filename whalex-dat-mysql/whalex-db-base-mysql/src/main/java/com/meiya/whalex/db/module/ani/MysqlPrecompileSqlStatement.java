package com.meiya.whalex.db.module.ani;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.sql.module.DefaultPrecompileSqlStatement;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MysqlPrecompileSqlStatement extends DefaultPrecompileSqlStatement {

    /**
     * 参数位置交换索引映射
     */
    private Map<Integer, Integer> paramPositionSwapMap;

    public MysqlPrecompileSqlStatement(String sql, Map paramPositionSwapMap) {
        super(sql);
        this.paramPositionSwapMap =  paramPositionSwapMap;
    }

    @Override
    public void paramHandle(List<Object> params) {

        if(CollectionUtil.isNotEmpty(paramPositionSwapMap)) {

            Set<Integer> keySet = paramPositionSwapMap.keySet();

            for (Integer index1 : keySet) {

                Integer index2 = paramPositionSwapMap.get(index1);

                Object param1 = params.get(index1);
                Object param2 = params.get(index2);
                params.set(index1, param2);
                params.set(index2, param1);
            }

        }

    }
}
