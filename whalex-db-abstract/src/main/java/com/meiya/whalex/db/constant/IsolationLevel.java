package com.meiya.whalex.db.constant;

/**
 * 事务级别
 *
 * @author 黄河森
 * @date 2023/6/8
 * @package com.meiya.whalex.db.constant
 * @project whalex-data-driver
 */
public enum  IsolationLevel {
    /**
     * 默认事务级别
     */
    DEFAULT(-1),
    /**
     * 读未提交
     */
    READ_UNCOMMITTED(1),
    /**
     * 读已提交
     */
    READ_COMMITTED(2),
    /**
     * 可重复读
     */
    REPEATABLE_READ(4),
    /**
     * 序列化读
     */
    SERIALIZABLE(8);

    private final int value;

    IsolationLevel(int value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }

    public static IsolationLevel parser(int level) {
        IsolationLevel[] values = IsolationLevel.values();
        for (IsolationLevel isolationLevel : values) {
            if (isolationLevel.value == level) {
                return isolationLevel;
            }
        }
        throw new RuntimeException("无法识别事务级别: " + level + " , 请修改配置!");
    }

}
