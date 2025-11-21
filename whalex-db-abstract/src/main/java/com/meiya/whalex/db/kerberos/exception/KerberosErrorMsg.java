package com.meiya.whalex.db.kerberos.exception;

/**
 * @author 黄河森
 * @date 2023/5/31
 * @package com.meiya.whalex.db.kerberos.exception
 * @project whalex-data-driver
 */
public interface KerberosErrorMsg {

    /**
     * HD 系列组件认证登录异常信息
     */
    String HD_LOGIN_FAIL = "login failed with {} and {} ." +
            "\nperhaps cause 1 is (wrong password) keytab file and user not match, you can kinit -k -t keytab user in client server to check." +
            "\nperhaps cause 2 is (clock skew) time of local server and remote server not match, please check ntp to remote server." +
            "\nperhaps cause 3 is (aes256 not support) aes256 not support by default jdk/jre, need copy local_policy.jar and US_export_policy.jar from remote server in path /opt/huawei/Bigdata/jdk/jre/lib/security." +
            "\nperhaps cause 4 is (no rule) principal format not support by default, need add property hadoop.security.auth_to_local(in core-site.xml) value RULE:[1:$1] RULE:[2:$1]." +
            "\nperhaps cause 5 is (time out) can not connect to kdc server or there is fire wall in the network.";

}
