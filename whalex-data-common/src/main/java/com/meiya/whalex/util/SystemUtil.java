package com.meiya.whalex.util;

import cn.hutool.system.RuntimeInfo;

/**
 * 系统操作类
 *
 * @author 黄河森
 * @date 2019/10/21
 * @project whale-cloud-platformX
 */
public class SystemUtil {

    private static final RuntimeInfo runtimeInfo = new RuntimeInfo();

    /**
     * 获取jvm内存使用率
     *
     * @return
     */
    public static double getMemoryUsage() {
        return (double) (runtimeInfo.getTotalMemory() - runtimeInfo.getFreeMemory()) / runtimeInfo.getMaxMemory();
    }

}
