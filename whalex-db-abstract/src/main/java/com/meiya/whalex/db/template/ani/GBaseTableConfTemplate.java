package com.meiya.whalex.db.template.ani;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * MySql 数据库表模板
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class GBaseTableConfTemplate {

    private String tableName;
}
