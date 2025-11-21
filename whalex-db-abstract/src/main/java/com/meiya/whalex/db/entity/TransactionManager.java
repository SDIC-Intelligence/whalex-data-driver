package com.meiya.whalex.db.entity;

/**
 * 事务管理对象
 *
 * 每个组件都必须实现该事务管理接口
 *
 * @param <T> 对应组件事务管理对象，比如 mysql 就是 Connection
 * @author 黄河森
 * @date 2022/4/30
 * @package com.meiya.whalex.db.entity
 * @project whalex-data-driver
 */
public interface TransactionManager<T> {

    /**
     * 获取事务连接对象
     *
     * @return
     */
    T getTransactionClient();

    /**
     * 事务回滚
     *
     * @throws Exception
     */
    void rollback() throws Exception;

    /**
     * 事务提交
     *
     * @throws Exception
     */
    void commit() throws Exception;

    /**
     * 事务长时间未操作导致超时时触发
     *
     * @throws Exception
     */
    void transactionTimeOut() throws Exception;

}
