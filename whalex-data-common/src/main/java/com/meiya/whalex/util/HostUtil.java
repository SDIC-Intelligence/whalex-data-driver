package com.meiya.whalex.util;

import cn.hutool.system.HostInfo;

/**
 * 服务器信息工具类
 *
 * @author 黄河森
 * @date 2020/4/17
 * @project whale-cloud-platformX
 */
public class HostUtil {

    private static final HostInfo HOST_INFO = new HostInfo();

    public static final String getAddress() {
        return HOST_INFO.getAddress();
    }

}
