package com.meiya.whalex.jdbc.parser;

import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.db.entity.DropSequenceBean;
import com.meiya.whalex.sql.module.RdbmsModuleService;
import com.meiya.whalex.sql2dsl.parser.DropSequenceSqlToDslParser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang3.StringUtils;

import java.util.Queue;

/**
 * 创建表sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class DropSequenceSqlParser implements ISqlParser<Boolean> {

    DbModuleService service;

    DatabaseSetting databaseSetting;

    private String sql;

    DropSequenceBean dropSequenceBean;

    public DropSequenceSqlParser(String sql, DbModuleService service, DatabaseSetting databaseSetting) {
        this.sql = sql;
        this.service = service;
        this.databaseSetting = databaseSetting;
        dropSequenceBean = new DropSequenceBean();
    }




    @Override
    public Boolean execute() throws Exception {
        PageResult pageResult = executeAndGetPageResult();
        return pageResult.getSuccess();
    }

    @Override
    public PageResult executeAndGetPageResult() throws Exception {
        if(!(service instanceof RdbmsModuleService)) {
            throw new RuntimeException("序列操作，只支持关系型数据库");
        }

        DropSequenceSqlToDslParser parser = new DropSequenceSqlToDslParser(sql);
        DropSequenceBean condition = parser.handle();

        RdbmsModuleService rdbmsModuleService = (RdbmsModuleService) service;
        PageResult pageResult = rdbmsModuleService.dropSequence(databaseSetting, condition);
        return pageResult;
    }


}
