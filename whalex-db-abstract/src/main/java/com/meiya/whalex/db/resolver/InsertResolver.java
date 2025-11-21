package com.meiya.whalex.db.resolver;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.exception.BusinessException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 新增入库结果解析
 *
 * @author 黄河森
 * @date 2023/8/4
 * @package com.meiya.whalex.db.resolver
 * @project whalex-data-driver
 * @description InsertResolver
 */
public class InsertResolver {

    private PageResult pageResult;

    private InsertResolver(PageResult pageResult) {
        this.pageResult = pageResult;
    }

    public static InsertResolver resolver(PageResult pageResult) {
        return new InsertResolver(pageResult);
    }

    public List<Object> analysisForGenerateKey() throws Exception {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        List<Map<String, Object>> rows = this.pageResult.getRows();
        if (CollectionUtil.isNotEmpty(rows)) {
            List<Object> generatedKeys = rows.stream().flatMap(row -> {
                Object key = row.get("GENERATED_KEY");
                if (key == null) {
                    return null;
                } else {
                    return Stream.of(key);
                }
            }).collect(Collectors.toList());
            return generatedKeys;
        } else {
            return com.meiya.whalex.util.collection.CollectionUtil.EMPTY_LIST;
        }
    }

    public List<Map<String, Object>> analysisForReturnVal() throws Exception {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        List<Map<String, Object>> rows = this.pageResult.getRows();
        return rows;
    }

}
