package com.meiya.whalex.sql2dsl.entity;

import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * SQL 建表解析返回对象
 *
 * @author 黄河森
 * @date 2024/4/19
 * @package com.meiya.whalex.sql2dsl.entity
 * @project whalex-data-driver
 * @description SqlCreateTable
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SqlCreateTable {

    /**
     * 建表参数
     */
    private CreateTableParamCondition createTable;

    /**
     * 建索引参数
     */
    private List<IndexParamCondition> createIndex;

}
