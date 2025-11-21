package com.meiya.whalex.db.resolver;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.interior.db.search.condition.Sort;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author 黄河森
 * @date 2023/6/14
 * @package com.meiya.whalex.db.resolver
 * @project whalex-data-driver
 */
public class IndexesResolver {

    List<IndexesResolver.IndexesResult> results;

    private PageResult pageResult;

    private IndexesResolver(PageResult pageResult) {
        this.pageResult = pageResult;
    }

    /**
     * 解析器
     *
     * @param pageResult
     * @return
     */
    public static IndexesResolver resolver(PageResult pageResult) {
        return new IndexesResolver(pageResult);
    }

    public List<IndexesResolver.IndexesResult> analysis() {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        List<Map<String, Object>> rows = this.pageResult.getRows();
        if (CollectionUtil.isNotEmpty(rows)) {
            List<IndexesResolver.IndexesResult> collect = rows.stream().flatMap(row -> {

                LinkedHashMap<String, String> columns = MapUtil.get(row, "columns", LinkedHashMap.class);

                LinkedHashMap<String, Sort> newColumns = new LinkedHashMap<>(columns.size());

                for (Map.Entry<String, String> entry : columns.entrySet()) {
                    String value = entry.getValue();
                    Sort sort = null;
                    if (StringUtils.isNotBlank(value)) {
                        sort = Sort.parse(value);
                    }
                    newColumns.put(entry.getKey(), sort);
                }

                return Stream.of(IndexesResult.builder()
                                .indexName(MapUtil.getStr(row, "indexName", null))
                                .columns(newColumns)
                                .unique(MapUtil.getBool(row, "isUnique", null))
                                .primaryKey(MapUtil.getBool(row, "isPrimaryKey", false))
                                .build());
                    }
            ).collect(Collectors.toList());
            results = collect;
        } else {
            results = com.meiya.whalex.util.collection.CollectionUtil.EMPTY_LIST;
        }
        return results;
    }

    @Data
    @Builder
    public static class IndexesResult {
        /**
         * 索引名
         */
        private String indexName;

        /**
         * 索引字段
         */
        private LinkedHashMap<String, Sort> columns;

        /**
         * 是否唯一索引
         */
        private Boolean unique;

        /**
         * 是否主键
         */
        private Boolean primaryKey;
    }
}
