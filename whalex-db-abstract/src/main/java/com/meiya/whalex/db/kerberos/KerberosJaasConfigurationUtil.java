package com.meiya.whalex.db.kerberos;

import lombok.extern.slf4j.Slf4j;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kerberos 认证环境变量设置
 *
 * @author 黄河森
 * @date 2023/3/14
 * @package com.meiya.whalex.db.util.common
 * @project whalex-data-driver
 */
@Slf4j
public class KerberosJaasConfigurationUtil {

    public static final String JAAS_POSTFIX = ".jaas.conf";

    /**
     * is IBM jdk or not
     */
    public static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");

    /**
     * JAAS 配置模块
     */
    public enum Module {
        GREMLIN("gremlinclient"), KAFKA("KafkaClient"), ES("EsClient"), CLIENT("Client"), HETU("HetuClient");

        private String name;

        Module(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static synchronized void setJaasConfig(String principal, String keytabPath, Module module) throws IOException {
        JaasConfiguration jaasConfiguration;
        Configuration configuration = Configuration.getConfiguration();
        if (configuration instanceof JaasConfiguration) {
            jaasConfiguration = (JaasConfiguration) configuration;
        } else {
            jaasConfiguration = new JaasConfiguration();
            Configuration.setConfiguration(jaasConfiguration);
        }
        //windows路径下分隔符替换
        keytabPath = keytabPath.replace("\\", "\\\\");
        jaasConfiguration.addEntry(module.getName(), principal, keytabPath);
    }

    public static String getKrb5LoginModuleName() {
        return System.getProperty("java.vendor").contains("IBM") ? "com.ibm.security.auth.module.Krb5LoginModule" : "com.sun.security.auth.module.Krb5LoginModule";
    }

    /**
     * copy from hbase zkutil 0.94&0.98 A JAAS configuration that defines the login modules that we want to use for
     * login.
     */
    public static class JaasConfiguration extends javax.security.auth.login.Configuration {

        /**
         * JAAS 配置列表
         */
        private final Map<String, AppConfigurationEntry[]> KEYTAB_KERBEROS_CONF = new ConcurrentHashMap<>();

        private javax.security.auth.login.Configuration baseConfig;

        public JaasConfiguration() {
            try {
                this.baseConfig = javax.security.auth.login.Configuration.getConfiguration();
            } catch (SecurityException e) {
                this.baseConfig = null;
            }
        }

        public void addEntry(String entryName, String principal, String keytabFile) throws IOException {
            boolean useTicketCache = keytabFile == null || keytabFile.length() == 0;
            Map<String, String> kerberosOption = initKerberosOption(principal, keytabFile, useTicketCache);
            AppConfigurationEntry appConfigurationEntry = new AppConfigurationEntry(
                    getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, kerberosOption);
            KEYTAB_KERBEROS_CONF.put(entryName,  new AppConfigurationEntry[]{appConfigurationEntry});
            log.info("JaasConfiguration loginContextName=" + entryName + " principal=" + principal
                    + " useTicketCache=" + useTicketCache + " keytabFile=" + keytabFile);
        }

        private Map<String, String> initKerberosOption(String principal, String keytabFile, boolean useTicketCache) throws IOException {
            Map<String, String> kerberosOption = new HashMap<>();
            if (IS_IBM_JDK) {
                kerberosOption.put("credsType", "both");
                kerberosOption.put("useKeytab", keytabFile);
            } else {
                kerberosOption.put("useKeyTab", "true");
                kerberosOption.put("useTicketCache", String.valueOf(useTicketCache));
                kerberosOption.put("doNotPrompt", "true");
                kerberosOption.put("storeKey", "true");
                kerberosOption.put("keyTab", keytabFile);
            }
            kerberosOption.put("principal", principal);
            kerberosOption.put("refreshKrb5Config", "true");
            kerberosOption.put("debug", "true");
            return kerberosOption;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
            if (KEYTAB_KERBEROS_CONF.containsKey(appName)) {
                return KEYTAB_KERBEROS_CONF.get(appName);
            }
            if (baseConfig != null) {
                return baseConfig.getAppConfigurationEntry(appName);
            }
            return (null);
        }
    }

}
