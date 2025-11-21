package com.meiya.whalex.interior.db.builder;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.Where;

/**
 * Where AND 检索条件对象构造器
 *
 * @author 黄河森
 * @date 2021/6/17
 * @project whalex-data-driver-back
 */
public class OrWhereBuilder extends NestWhereBuilder {

    private OrWhereBuilder() {
        super(Rel.OR);
    }

    public static OrWhereBuilder builder() {
        return new OrWhereBuilder();
    }

    public NestWhereBuilder or(Where... subWhere) {
        where.getParams().addAll(CollectionUtil.newArrayList(subWhere));
        return this;
    }
}
