package com.meiya.whalex.jdbc.parser;

import com.meiya.whalex.db.builder.TableConfigurationBuilder;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.sql2dsl.parser.DropIndexSqlToDslParser;
import org.apache.commons.lang3.StringUtils;

/**
 * 索引删除sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class DropIndexSqlParser implements ISqlParser<Boolean> {

    private String sql;

    DbModuleService service;

    DatabaseSetting databaseSetting;

    public DropIndexSqlParser(String sql, DbModuleService service, DatabaseSetting databaseSetting) {
        this.sql = sql.trim();
        this.service = service;
        this.databaseSetting = databaseSetting;
    }


    @Override
    public Boolean execute() throws Exception {

        DropIndexSqlToDslParser parser = new DropIndexSqlToDslParser(sql);
        IndexParamCondition condition = parser.handle();
        String tableName = parser.getTableName();

        if(StringUtils.isBlank(tableName)) {
            throw new RuntimeException("表名不能为空");
        }

        TableSetting tableSetting = TableConfigurationBuilder.builder().tableName(tableName).build();
        PageResult pageResult = service.deleteIndex(databaseSetting, tableSetting, condition);
        return pageResult.getSuccess();

    }

    @Override
    public PageResult executeAndGetPageResult() throws Exception {
        throw new RuntimeException("等待阿森哥的实现");
    }

}
