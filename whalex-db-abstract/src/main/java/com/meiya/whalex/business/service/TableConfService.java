package com.meiya.whalex.business.service;

import com.meiya.whalex.business.entity.TableConf;

import java.util.Date;
import java.util.List;

/**
 * 数据库表配置业务层
 * @author 黄河森
 * @date 2019/9/20
 * @project whale-cloud-platformX
 */
public interface TableConfService {

    /**
     * 获取数据库表信息最新更新时间
     * @param dbType
     * @return
     */
    Date queryLastUpdateTimeForTable(String dbType);

    /**
     * 根据组件类型和更新时间获取表信息
     * @param dbType
     * @param updateTime
     * @return
     */
    List<TableConf> queryListByUpdateTimeAndDbType(String dbType, Date updateTime);

    /**
     * 根据数据库标识符查询表配置
     *
     * @param bigResourceId
     * @return
     */
    List<TableConf> queryListByBigResourceId(List<String> bigResourceId);

    /**
     * 根据主键查询表配置
     *
     * @param id
     * @return
     */
    TableConf queryOneById(String id);

    /**
     * 根据数据库配置标识符查询对应的数据库表配置
     *
     * @param bigResourceId
     * @return
     */
    List<TableConf> queryListByBigResourceId(String bigResourceId);

    /**
     * 获取 schema 模板
     *
     * @param schemaName
     * @param dbType
     * @return
     */
    String querySchemaTemPlate(String schemaName, String dbType);

}
