package com.meiya.whalex.interior.db.builder;

import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.interior.db.search.in.Hint;

/**
 * @author 黄河森
 * @date 2022/3/2
 * @package com.meiya.whalex.interior.db.builder
 * @project whalex-data-driver
 */
public class QueryHintBuilder {

    private Hint hint;

    private QueryParamBuilder builder;

    private QueryHintBuilder(QueryParamBuilder builder, Hint hint) {
        this.builder = builder;
        this.hint = hint;
    }

    public static QueryHintBuilder builder(QueryParamBuilder builder, Hint hint) {
        return new QueryHintBuilder(builder, hint);
    }

    public QueryParamBuilder returned() {
        return builder;
    }

    public QueryHintBuilder indexName(String indexName) {
        this.hint.setIndexName(indexName);
        return this;
    }

    public QueryHintBuilder index(String column, Sort sort) {
        Hint.HintIndex hintIndex = new Hint.HintIndex();
        this.hint.setIndex(hintIndex);
        hintIndex.setColumn(column);
        if (sort != null) {
            hintIndex.setSort(sort);
        }
        return this;
    }
}
