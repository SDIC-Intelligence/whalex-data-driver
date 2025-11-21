package com.meiya.whalex.jdbc.factory;

import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.jdbc.parser.AlterTableSqlParser;
import com.meiya.whalex.jdbc.parser.CommentSqlParser;
import com.meiya.whalex.jdbc.parser.CreateDatabaseSqlParser;
import com.meiya.whalex.jdbc.parser.CreateIndexSqlParser;
import com.meiya.whalex.jdbc.parser.CreateSequenceSqlParser;
import com.meiya.whalex.jdbc.parser.CreateTableSqlParser;
import com.meiya.whalex.jdbc.parser.CreateViewSqlParser;
import com.meiya.whalex.jdbc.parser.DeleteSqlParser;
import com.meiya.whalex.jdbc.parser.DescTableSqlParser;
import com.meiya.whalex.jdbc.parser.DescribeSqlParser;
import com.meiya.whalex.jdbc.parser.DropDatabaseSqlParser;
import com.meiya.whalex.jdbc.parser.DropIndexSqlParser;
import com.meiya.whalex.jdbc.parser.DropSequenceSqlParser;
import com.meiya.whalex.jdbc.parser.DropTableSqlParser;
import com.meiya.whalex.jdbc.parser.DropViewSqlParser;
import com.meiya.whalex.jdbc.parser.ISqlParser;
import com.meiya.whalex.jdbc.parser.InsertSqlParser;
import com.meiya.whalex.jdbc.parser.QuerySqlParser;
import com.meiya.whalex.jdbc.parser.ShowSqlParser;
import com.meiya.whalex.jdbc.parser.UpdateSqlParser;

import java.sql.SQLException;


/**
 * sql解析工厂
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class SqlParserFactory {

    private SqlParserFactory() {}

    public static ISqlParser createSqlParser(String sql,
                                             Object[] params,
                                             DbModuleService service,
                                             DatabaseSetting databaseSetting,
                                             int maxRows) throws SQLException {

        String[] split = sql.trim().split("\\s+");


        //select
        if(split[0].equalsIgnoreCase("select")) {
            return new QuerySqlParser(sql, params, service, databaseSetting, maxRows);
        }

        //insert
        if(split[0].equalsIgnoreCase("insert")) {
            return new InsertSqlParser(sql, params, service, databaseSetting);
        }


        //update
        if(split[0].equalsIgnoreCase("update")) {
            return new UpdateSqlParser(sql, params, service, databaseSetting);
        }

        //delete
        if(split[0].equalsIgnoreCase("delete")) {
            return new DeleteSqlParser(sql, params, service, databaseSetting);
        }

        //create table
        if(split[0].equalsIgnoreCase("create") && split[1].equalsIgnoreCase("table")) {
            return new CreateTableSqlParser(sql, service, databaseSetting);
        }

        //drop table
        if(split[0].equalsIgnoreCase("drop") && split[1].equalsIgnoreCase("table")) {
            return new DropTableSqlParser(sql, service, databaseSetting);
        }

        //create view or CREATE OR REPLACE VIEW
        if((split[0].equalsIgnoreCase("create") && split[1].equalsIgnoreCase("view"))
                || (split[0].equalsIgnoreCase("create") && split[1].equalsIgnoreCase("or") && split[2].equalsIgnoreCase("replace") && split[3].equalsIgnoreCase("view"))) {
            return new CreateViewSqlParser(sql, params, service, databaseSetting);
        }

        //drop view
        if(split[0].equalsIgnoreCase("drop") && split[1].equalsIgnoreCase("view")) {
            return new DropViewSqlParser(sql, service, databaseSetting);
        }

        //create database
        if(split[0].equalsIgnoreCase("create") && split[1].equalsIgnoreCase("database")) {
            return new CreateDatabaseSqlParser(sql, service, databaseSetting);
        }

        //drop database
        if(split[0].equalsIgnoreCase("drop") && split[1].equalsIgnoreCase("database")) {
            return new DropDatabaseSqlParser(sql, service, databaseSetting);
        }

        //show
        if(split[0].equalsIgnoreCase("show")){
            return new ShowSqlParser(sql, service, databaseSetting);
        }

        //create index
        if(split[0].equalsIgnoreCase("create")
                && split[1].equalsIgnoreCase("index")) {
            return new CreateIndexSqlParser(sql, service, databaseSetting);
        }

        //create index
        if(split[0].equalsIgnoreCase("create")
                && split[1].equalsIgnoreCase("unique")
                && split[2].equalsIgnoreCase("index")) {
            return new CreateIndexSqlParser(sql, service, databaseSetting);
        }

        //drop index
        if(split[0].equalsIgnoreCase("drop")
                && split[1].equalsIgnoreCase("index")) {
            return new DropIndexSqlParser(sql, service, databaseSetting);
        }

        //alter table
        if(split[0].equalsIgnoreCase("alter")
                && split[1].equalsIgnoreCase("table")) {
            return new AlterTableSqlParser(sql, service, databaseSetting);
        }

        //alter table
        if(split[0].equalsIgnoreCase("comment")
                && split[1].equalsIgnoreCase("on")) {
            return new CommentSqlParser(sql, service, databaseSetting);
        }

        //alter table
        if(split[0].equalsIgnoreCase("desc")) {
            return new DescTableSqlParser(sql, service, databaseSetting);
        }

        //create sequence
        if(split[0].equalsIgnoreCase("create")
                && split[1].equalsIgnoreCase("sequence")) {
            return new CreateSequenceSqlParser(sql, service, databaseSetting);
        }

        //drop sequence
        if(split[0].equalsIgnoreCase("drop")
                && split[1].equalsIgnoreCase("sequence")) {
            return new DropSequenceSqlParser(sql, service, databaseSetting);
        }

        //describe
        if(split[0].equalsIgnoreCase("describe")) {
            return new DescribeSqlParser(sql, service, databaseSetting);
        }

        throw new SQLException("未知的sql:" + split[0]);
    }
}
