package com.meiya.whalex.db.entity.graph;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.db.kerberos.KerberosUniformAuth;
import com.meiya.whalex.exception.BusinessException;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Gremlin 客户端
 *
 * @author 黄河森
 * @date 2022/12/01
 * @project whalex-data-driver
 */
@Slf4j
@Getter
public class GremlinDriverClient extends GremlinClient {

    @Builder
    GremlinDriverClient(String hosts
            , Integer port
            , String username
            , String password
            , String alias
            , Integer minConnectionPoolSize
            , Integer maxConnectionPoolSize
            , String serializerClass
            , String mapperBuilderClass
            , String ioRegistries
            , String protocol
            , Boolean enableSSL
            , Boolean sslSkipCertValidation
            , Integer maxContentLength
            , Integer resultIterationBatchSize
            , String jaasEntry
            , KerberosUniformAuth kerberosUniformLogin) {
        Cluster.Builder build = Cluster.build();
        String[] split = StringUtils.split(hosts, ",");
        build.addContactPoints(split);
        build.port(port);
        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            build.credentials(username, password);
        }
        if (maxConnectionPoolSize != null) {
            build.maxConnectionPoolSize(maxConnectionPoolSize);
            build.maxInProcessPerConnection(maxConnectionPoolSize);
        }
        if (minConnectionPoolSize != null) {
            build.minConnectionPoolSize(minConnectionPoolSize);
        }
        if (StringUtils.isNotBlank(serializerClass)) {
            try {
                MessageSerializer serializer = null;
                Mapper.Builder mapperBuilder = null;
                if (StringUtils.isNotBlank(mapperBuilderClass)) {
                    Class<?> aClass = Class.forName(mapperBuilderClass);
                    Constructor<?> declaredConstructor = aClass.getDeclaredConstructor();
                    declaredConstructor.setAccessible(true);
                    mapperBuilder = (Mapper.Builder) declaredConstructor.newInstance();
                    if (StringUtils.isNotBlank(ioRegistries)) {
                        List<String> ioRegistersStr = CollectionUtil.newArrayList(StringUtils.split(ioRegistries, ","));
                        List<IoRegistry> ioRegistryList = new ArrayList<>(ioRegistersStr.size());
                        for (String s : ioRegistersStr) {
                            Class<?> aClass1 = Class.forName(s);
                            Constructor<?> declaredConstructor1 = aClass1.getDeclaredConstructor();
                            declaredConstructor1.setAccessible(true);
                            IoRegistry ioRegistry = (IoRegistry) declaredConstructor1.newInstance();
                            ioRegistryList.add(ioRegistry);
                        }
                        mapperBuilder.addRegistries(ioRegistryList);
                    }
                }
                Class clazz = Class.forName(serializerClass);
                if (mapperBuilder != null) {
                    Constructor constructor = clazz.getConstructor(mapperBuilder.getClass());
                    serializer = (MessageSerializer) constructor.newInstance(mapperBuilder);
                } else {
                    serializer = (MessageSerializer)clazz.newInstance();
                }
                if (mapperBuilder == null && StringUtils.isNotBlank(ioRegistries)) {
                    Map<String, Object> config = new HashMap<>(1);
                    config.put("ioRegistries", CollectionUtil.newArrayList(StringUtils.split(ioRegistries, ",")));
                    serializer.configure(config, null);
                }
                build.serializer(serializer);
            } catch (Exception e) {
                throw new BusinessException("初始化配置图数据库序列化器失败!", e);
            }
        }
        if (StringUtils.isNotBlank(protocol)) {
            build.protocol(protocol);
        }
        if (enableSSL != null) {
            build.enableSsl(enableSSL);
        }
        if (sslSkipCertValidation != null) {
            build.sslSkipCertValidation(sslSkipCertValidation);
        }
        if (maxContentLength != null) {
            build.maxContentLength(maxContentLength);
        }
        if (resultIterationBatchSize != null) {
            build.resultIterationBatchSize(resultIterationBatchSize);
        }
        if (StringUtils.isNotBlank(jaasEntry)) {
            build.jaasEntry(jaasEntry);
        }
        super.kerberosUniformLogin = kerberosUniformLogin;
        cluster = build.create();
        client = getClient(alias);
    }

}
