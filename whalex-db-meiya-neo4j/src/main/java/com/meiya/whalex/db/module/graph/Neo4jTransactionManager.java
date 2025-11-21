package com.meiya.whalex.db.module.graph;

import com.meiya.whalex.db.entity.TransactionManager;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
/**
 * @author 黄河森
 * @date 2023/1/3
 * @package com.meiya.whalex.db.module.graph
 * @project whalex-data-driver
 */
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Neo4jTransactionManager implements TransactionManager<Neo4jTransaction> {

    private Neo4jTransaction transaction;

    private Neo4jSession session;

    @Override
    public Neo4jTransaction getTransactionClient() {
        return transaction;
    }

    @Override
    public void rollback() throws Exception {
        try {
            transaction.rollback();
        } finally {
            transaction.close();
            session.close();
        }
    }

    @Override
    public void commit() throws Exception {
        try {
            transaction.commit();
        } finally {
            transaction.close();
            session.close();
        }
    }

    @Override
    public void transactionTimeOut() throws Exception {
        rollback();
    }
}
