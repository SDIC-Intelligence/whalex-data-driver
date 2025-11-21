package com.meiya.whalex.business.service;

import com.meiya.whalex.business.entity.CertificateConf;
import com.meiya.whalex.business.entity.DatabaseConf;

import java.util.Date;
import java.util.List;

/**
 * 数据库配置信息表业务层
 * @author 黄河森
 * @date 2019/9/20
 * @project whale-cloud-platformX
 */
public interface DatabaseConfService {

    /**
     * 获取数据库信息最新更新时间
     * @param dbType
     * @return
     */
    Date queryLastUpdateTimeForDatabase(String dbType);

    /**
     * 查询数据库配置信息根据标识符
     *
     * @param bigResourceId
     * @return
     */
    DatabaseConf queryOneByBigResourceId(String bigResourceId);

    /**
     * 根据组件类型查询数据库配置信息
     *
     * 更新时间条件为可选择条件，若设置，则过滤出指定时间之后的配置
     *
     * @param dbType
     * @param updateTime
     * @return
     */
    List<DatabaseConf> queryListByDbType(String dbType, Date updateTime);


    /**
     * 根据 certificateId 获取证书
     *
     * @param certificateId
     * @return
     */
    CertificateConf queryCertificateConfById(String certificateId);

}
