package com.meiya.whalex.db.resolver;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.table.partition.TablePartitions;
import com.meiya.whalex.exception.BusinessException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 解析表相关信息返回结果体
 *
 * @author 黄河森
 * @date 2021/9/13
 * @package com.meiya.whalex.db.resolver
 * @project whalex-data-driver
 */
public class TablePartitionResolver {
    private PageResult pageResult;

    private TablePartitionResolver(PageResult pageResult) {
        this.pageResult = pageResult;
    }

    /**
     * 解析器
     *
     * @param pageResult
     * @return
     */
    public static TablePartitionResolver resolver(PageResult pageResult) {
        return new TablePartitionResolver(pageResult);
    }

    /**
     * 解析方法
     *
     * @return
     */
    public List<TablePartitions> analysis() {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        List<Map<String, Object>> rows = this.pageResult.getRows();
        if (CollectionUtil.isEmpty(rows)) {
            return null;
        }
        List<TablePartitions> tablePartitions = new ArrayList<>();

        for (Map<String, Object> row : rows) {
            TablePartitions tablePartition = BeanUtil.mapToBean(row, TablePartitions.class, false, CopyOptions.create());
            tablePartitions.add(tablePartition);
        }
        return tablePartitions;
    }
}
