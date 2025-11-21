package com.meiya.whalex.thread.entity;

/**
 * 云组件配置
 *
 * @author 黄河森
 * @date 2019/9/12
 * @project whale-cloud-platformX
 */
public class DbThreadBaseConfig {

    /**
     * 核心线程数
     */
    private Integer corePoolSize = 32;

    /**
     * 最大线程数
     */
    private Integer maximumPoolSize = 64;

    /**
     * 队列大小
     */
    private Integer dequeSize = 50;

    /**
     * 线程超时时间
     */
    private Integer timeOut = 60;

    /**
     * 扩展大小
     */
    private Integer extendSize = 10;

    /**
     * 阈值队列大小
     */
    private Integer adjustDequeSize = 1;

    public Integer getCorePoolSize() {
        return corePoolSize;
    }

    public void setCorePoolSize(Integer corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public Integer getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public void setMaximumPoolSize(Integer maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
    }

    public Integer getDequeSize() {
        return dequeSize;
    }

    public void setDequeSize(Integer dequeSize) {
        this.dequeSize = dequeSize;
    }

    public Integer getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(Integer timeOut) {
        this.timeOut = timeOut;
    }

    public Integer getExtendSize() {
        return extendSize;
    }

    public void setExtendSize(Integer extendSize) {
        this.extendSize = extendSize;
    }

    public Integer getAdjustDequeSize() {
        return adjustDequeSize;
    }

    public void setAdjustDequeSize(Integer adjustDequeSize) {
        this.adjustDequeSize = adjustDequeSize;
    }
}
