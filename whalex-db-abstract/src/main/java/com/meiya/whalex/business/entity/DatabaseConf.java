package com.meiya.whalex.business.entity;

import java.util.Date;

/**
 * 数据库链接信息
 *
 * @author 黄河森
 * @date 2019/9/17
 * @project whale-cloud-platformX
 */
public class DatabaseConf {

    private String bigdataResourceId;

    private String resourceName;

    private Integer isDel;

    private String createTime;

    private Date updateTime;

    private String securitySoftwareOrgcode;

    private String connTypeId;

    private String connSetting;

    public String getBigdataResourceId() {
        return bigdataResourceId;
    }

    public void setBigdataResourceId(String bigdataResourceId) {
        this.bigdataResourceId = bigdataResourceId;
    }

    public String getResourceName() {
        return resourceName;
    }

    public void setResourceName(String resourceName) {
        this.resourceName = resourceName;
    }

    public Integer getIsDel() {
        return isDel;
    }

    public void setIsDel(Integer isDel) {
        this.isDel = isDel;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getSecuritySoftwareOrgcode() {
        return securitySoftwareOrgcode;
    }

    public void setSecuritySoftwareOrgcode(String securitySoftwareOrgcode) {
        this.securitySoftwareOrgcode = securitySoftwareOrgcode;
    }

    public String getConnTypeId() {
        return connTypeId;
    }

    public void setConnTypeId(String connTypeId) {
        this.connTypeId = connTypeId;
    }

    public String getConnSetting() {
        return connSetting;
    }

    public void setConnSetting(String connSetting) {
        this.connSetting = connSetting;
    }

    public DatabaseConf() {
    }

    public DatabaseConf(String bigdataResourceId, String resourceName, Integer isDel, String createTime, Date updateTime, String securitySoftwareOrgcode, String connTypeId, String connSetting) {
        this.bigdataResourceId = bigdataResourceId;
        this.resourceName = resourceName;
        this.isDel = isDel;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.securitySoftwareOrgcode = securitySoftwareOrgcode;
        this.connTypeId = connTypeId;
        this.connSetting = connSetting;
    }
}
