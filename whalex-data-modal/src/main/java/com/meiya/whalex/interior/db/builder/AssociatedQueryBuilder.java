package com.meiya.whalex.interior.db.builder;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.interior.db.search.condition.AssociatedType;
import com.meiya.whalex.interior.db.search.in.AssociatedQuery;
import com.meiya.whalex.interior.db.search.in.Where;

/**
 * @author 黄河森
 * @date 2022/3/2
 * @package com.meiya.whalex.interior.db.builder
 * @project whalex-data-driver
 */
public class AssociatedQueryBuilder {

    private AssociatedQuery associatedQuery;

    private AssociatedQueryBuilder() {
        this.associatedQuery = new AssociatedQuery();
    }

    public static AssociatedQueryBuilder builder() {
        return new AssociatedQueryBuilder();
    }

    public AssociatedQuery build() {
        return associatedQuery;
    }

    public AssociatedQueryBuilder tableName(String tableName) {
        this.associatedQuery.setTableName(tableName);
        return this;
    }

    public AssociatedQueryBuilder type(AssociatedType type) {
        this.associatedQuery.setType(type);
        return this;
    }

    public AssociatedQueryBuilder select(String... fields) {
        this.associatedQuery.setSelect(CollectionUtil.newArrayList(fields));
        return this;
    }

    public AssociatedQueryBuilder localField(String localField) {
        this.associatedQuery.setLocalField(localField);
        return this;
    }

    public AssociatedQueryBuilder foreignField(String foreignField) {
        this.associatedQuery.setForeignField(foreignField);
        return this;
    }

    public AssociatedQueryBuilder where(Where where) {
        this.associatedQuery.setWhere(where);
        return this;
    }
}
