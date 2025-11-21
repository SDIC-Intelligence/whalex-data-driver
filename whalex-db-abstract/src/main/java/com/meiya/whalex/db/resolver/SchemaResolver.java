package com.meiya.whalex.db.resolver;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author 黄河森
 * @date 2021/6/21
 * @project whalex-data-driver-back
 */
public class SchemaResolver {

    private PageResult pageResult;

    private SchemaResolver(PageResult pageResult) {
        this.pageResult = pageResult;
    }

    public static SchemaResolver resolver(PageResult pageResult) {
        return new SchemaResolver(pageResult);
    }

    public List<Schema> analysis() {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        List<Map<String, Object>> rows = this.pageResult.getRows();
        if (CollectionUtil.isNotEmpty(rows)) {
            List<Schema> collect = rows.stream().flatMap(row -> {
                String colName = String.valueOf(row.get("col_name"));
                Integer autoincrement = row.get("autoincrement") == null ? null : (Integer) row.get("autoincrement");
                Integer isKey = row.get("isKey") == null ? null : (Integer) row.get("isKey");
                String comment = row.get("comment") == null ? null : (String) row.get("comment");
                String dataType = row.get("data_type") == null ? null : (String) row.get("data_type");
                Integer columnSize = StringUtils.isBlank((String) row.get("columnSize")) ? null : Integer.valueOf((String) row.get("columnSize"));
                Integer decimalDigits = StringUtils.isBlank( (String)row.get("decimalDigits")) ? null : Integer.valueOf((String) row.get("decimalDigits"));
                Integer nullable = row.get("nullable") == null ? null : (Integer) row.get("nullable");
                String columnDef = row.get("columnDef") == null ? null : (String) row.get("columnDef");
                ItemFieldTypeEnum std_data_type = row.get("std_data_type") == null ? null : ItemFieldTypeEnum.findFieldTypeEnum((String) row.get("std_data_type"));
                Schema schema = Schema.builder().colName(colName).columnDef(columnDef).columnSize(columnSize).decimalDigits(decimalDigits).comment(comment).dataType(dataType)
                        .autoincrement(autoincrement == null ? null : (autoincrement.equals(1) ? true : false))
                        .isKey(isKey == null ? null : (isKey.equals(1) ? true : false))
                        .nullable(nullable == null ? null : (nullable.equals(1) ? true : false))
                        .fmtDataType(std_data_type)
                        .build();
                return Stream.of(schema);
            }).collect(Collectors.toList());
            return collect;
        } else {
            return com.meiya.whalex.util.collection.CollectionUtil.EMPTY_LIST;
        }
    }

    @Data
    @Builder
    public static class Schema {
        // 字段名称
        private String colName;
        // 是否自增
        private Boolean autoincrement;
        // 是否主键
        private Boolean isKey;
        // 描述
        private String comment;
        // 原始数据类型
        private String dataType;
        // 字段长度
        private Integer columnSize;
        // 字段小数位长度
        private Integer decimalDigits;
        // 是否非空
        private Boolean nullable;
        // 默认值
        private String columnDef;
        // 标准数据类型（DAT 统一数据类型）
        private ItemFieldTypeEnum fmtDataType;
    }
}
