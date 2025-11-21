package com.meiya.whalex.db.entity.cache;

import com.meiya.whalex.db.entity.AbstractDbHandler;
import com.meiya.whalex.interior.db.search.condition.Rel;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.function.Consumer;

/**
 * redis 组件操作实体类
 *
 * @author chenjp
 * @date 2020/9/7
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class RedisHandler extends AbstractDbHandler {

    private RedisQuery redisQuery;
    private List<RedisInsert> redisInsertList;
    private RedisDel redisDel;
    private RedisUpdate redisUpdate;
    private RedisPublishData redisPublishData;
    private RedisSubscribeData redisSubscribeData;


    @Data
    public static class RedisQuery {
        private String key;
        private String field;
        private List<String> fields;
        private Rel rel;
    }

    @Data
    public static class RedisInsert {
        private String key;
        private String field;
        private Object value;
        private Long expire;
        private Rel rel;
    }

    @Data
    public static class RedisDel {
        private String key;
        private String field;
        private List<String> fields;
        private Rel rel;
    }

    @Data
    public static class RedisUpdate {
        private String key;
        private Object value;
        private Long expire;
    }

    @Data
    public static class RedisPublishData {
        private Object value;
    }

    @Data
    public static class RedisSubscribeData {
        private Consumer<Object> consumer;
        private boolean async;
    }

}
