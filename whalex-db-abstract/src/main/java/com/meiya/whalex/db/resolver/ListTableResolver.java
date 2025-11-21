package com.meiya.whalex.db.resolver;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ObjectUtil;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.exception.BusinessException;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author 黄河森
 * @date 2021/6/21
 * @project whalex-data-driver-back
 */
@Data
public class ListTableResolver {

    private List<ListTableResult> results;

    private PageResult pageResult;

    private ListTableResolver(PageResult pageResult) {
        this.pageResult = pageResult;
    }

    public static ListTableResolver resolver(PageResult pageResult) {
        return new ListTableResolver(pageResult);
    }

    public List<ListTableResult> analysis() {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        List<Map<String, Object>> rows = this.pageResult.getRows();
        if (CollectionUtil.isNotEmpty(rows)) {
            List<ListTableResult> collect = rows.stream().flatMap(row -> Stream.of(ListTableResult.builder()
                    .tableName(ObjectUtil.toString(row.get("tableName")))
                    .schemaName(ObjectUtil.toString(row.get("schemaName")))
                    .tableComment(ObjectUtil.toString(row.get("tableComment")))
                    .build())).collect(Collectors.toList());
            results = collect;
        } else {
            results = com.meiya.whalex.util.collection.CollectionUtil.EMPTY_LIST;
        }
        return results;
    }

    @Data
    @Builder
    public static class ListTableResult {
        private String tableName;

        private String tableComment;

        private String schemaName;
    }

}
