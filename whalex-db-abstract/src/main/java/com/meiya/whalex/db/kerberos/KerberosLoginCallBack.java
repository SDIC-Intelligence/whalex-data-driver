package com.meiya.whalex.db.kerberos;


/**
 * kerberos 认证登录个性化扩展方法
 *
 * @author 黄河森
 * @date 2023/5/29
 * @package com.meiya.whalex.db.kerberos
 * @project whalex-data-driver
 */
public interface KerberosLoginCallBack {

    /**
     * kerberos 认证登录扩展方法
     *
     * @param kerberosUniformLogin
     * @throws Exception
     */
    void loginExtend(KerberosUniformAuth kerberosUniformLogin) throws Exception;

}
