package com.meiya.whalex.db.entity.cache;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterCommand;

import java.util.Set;

public class JedisClusterExt extends JedisCluster {
    public JedisClusterExt(HostAndPort node) {
        super(node);
    }

    public JedisClusterExt(HostAndPort node, int timeout) {
        super(node, timeout);
    }

    public JedisClusterExt(HostAndPort node, int timeout, int maxAttempts) {
        super(node, timeout, maxAttempts);
    }

    public JedisClusterExt(HostAndPort node, GenericObjectPoolConfig poolConfig) {
        super(node, poolConfig);
    }

    public JedisClusterExt(HostAndPort node, int timeout, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, poolConfig);
    }

    public JedisClusterExt(HostAndPort node, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, maxAttempts, poolConfig);
    }

    public JedisClusterExt(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public JedisClusterExt(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public JedisClusterExt(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }

    public JedisClusterExt(Set<HostAndPort> nodes) {
        super(nodes);
    }

    public JedisClusterExt(Set<HostAndPort> nodes, int timeout) {
        super(nodes, timeout);
    }

    public JedisClusterExt(Set<HostAndPort> nodes, int timeout, int maxAttempts) {
        super(nodes, timeout, maxAttempts);
    }

    public JedisClusterExt(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig) {
        super(nodes, poolConfig);
    }

    public JedisClusterExt(Set<HostAndPort> nodes, int timeout, GenericObjectPoolConfig poolConfig) {
        super(nodes, timeout, poolConfig);
    }

    public JedisClusterExt(Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, timeout, maxAttempts, poolConfig);
    }

    public JedisClusterExt(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public JedisClusterExt(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public JedisClusterExt(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }

    public JedisClusterExt(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String username, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, username, password, clientName, poolConfig);
    }

    public Set<String> keys(final String key) {
        return (Set)(new JedisClusterCommand<Set<String>>(this.connectionHandler, this.maxAttempts) {
            public Set<String> execute(Jedis connection) {
                return connection.keys(key);
            }
        }).run(key);
    }
}
