package com.meiya.whalex.sql.module;


import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.CreateSequenceBean;
import com.meiya.whalex.db.entity.DropSequenceBean;

import java.util.List;

/**
 * 数据库组件复杂sql查询接口
 *
 * @author 蔡荣桂
 * @date 2011/06/28
 * @project whale-cloud-platformX
 */
public interface RdbmsModuleService {

    /**
     * SQL 查询接口
     *
     * @param databaseSetting
     * @param sql
     * @param params
     * @return
     * @throws Exception
     */
    PageResult queryBySql(DatabaseSetting databaseSetting, String sql, List<Object> params) throws Exception;

    /**
     * SQL 更新接口
     *
     * @param databaseSetting
     * @param sql
     * @param params
     * @return
     * @throws Exception
     */
    PageResult updateBySql(DatabaseSetting databaseSetting, String sql, List<Object> params) throws Exception;

    /**
     * 新增序列接口
     * @param databaseSetting
     * @param createSequenceBean
     * @return
     * @throws Exception
     */
    PageResult createSequence(DatabaseSetting databaseSetting, CreateSequenceBean createSequenceBean) throws Exception;

    /**
     * 删除序列接口
     * @param databaseSetting
     * @param dropSequenceBean
     * @return
     * @throws Exception
     */
    PageResult dropSequence(DatabaseSetting databaseSetting, DropSequenceBean dropSequenceBean) throws Exception;

}
