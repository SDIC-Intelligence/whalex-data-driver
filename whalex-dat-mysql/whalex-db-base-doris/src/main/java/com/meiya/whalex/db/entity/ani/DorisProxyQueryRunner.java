package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.ResultSetHandler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 因为 Doris 可以通过 http 方式交互，因此重新定义这个链接，把 HTTP 连接池对象放进来统一管理
 *
 * @author 黄河森
 * @date 2024/6/28
 * @package com.meiya.whalex.db.entity.ani
 * @project whalex-data-driver
 * @description DorisQueryRunner
 */
@Slf4j
public class DorisProxyQueryRunner extends DorisQueryRunner implements AutoCloseable {

    List<DorisQueryRunner> queryRunners;

    Random random = new Random();

    List<DorisQueryRunner> exceptionQueryRunners = new ArrayList<>();

    private boolean flag = true;

//    public DorisProxyQueryRunner(List<DorisQueryRunner> queryRunners) {
//        this.queryRunners = queryRunners;
//        Thread testConnectionThread = new Thread(() -> {
//            while (flag) {
//                for (int i = 0; i < queryRunners.size(); i++) {
//                    DorisQueryRunner queryRunner = queryRunners.get(i);
//                    try {
//                        Connection connection = queryRunner.getDataSource().getConnection();
//                        connection.close();
//                    } catch (Exception throwables) {
//                        exceptionQueryRunners.add(queryRunner);
//                        queryRunners.remove(queryRunner);
//                        String ipPort = queryRunner.getIpPort();
//                        log.warn("集群结点[{}]异常", ipPort);
////                        System.out.println(String.format("集群结点[%s]异常", ipPort));
//                    }
//                }
//                try {
//                    Thread.sleep(1000L);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        testConnectionThread.setName("集群健康检测线程");
//        testConnectionThread.start();
//        Thread exceptionConnectionThread = new Thread(() -> {
//            while (flag) {
//                for (int i = 0; i < exceptionQueryRunners.size(); i++) {
//                    DorisQueryRunner exceptionQueryRunner = exceptionQueryRunners.get(i);
//                    try {
//                        Connection connection = exceptionQueryRunner.getDataSource().getConnection();
//                        connection.close();
//                        exceptionQueryRunners.remove(exceptionQueryRunner);
//                        queryRunners.add(exceptionQueryRunner);
//                        log.info("集群结点[{}]恢复", exceptionQueryRunner.getIpPort());
////                        System.out.println(String.format("集群结点[%s]恢复", exceptionQueryRunner.getIpPort()));
//                    } catch (Exception throwables) {
//                    }
//                }
//                if (exceptionQueryRunners.size() > 0) {
//                    //打印出异常结点信息
//                    StringBuilder sb = new StringBuilder();
//                    for (DorisQueryRunner exceptionQueryRunner : exceptionQueryRunners) {
//                        if (sb.length() > 0) {
//                            sb.append(", ");
//                        }
//                        sb.append(exceptionQueryRunner.getIpPort());
//                    }
//                    log.warn("集群结点[{}]异常", sb.toString());
////                    System.out.println(String.format("集群结点[%s]异常", sb.toString()));
//                }
//                try {
//                    Thread.sleep(1000L);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        exceptionConnectionThread.setName("集群异常恢复检测线程");
//        exceptionConnectionThread.start();
//    }

    private DorisQueryRunner getQueryRunner() {
        if(queryRunners.size() == 0) {
            throw new BusinessException("集群无可用连接，请检查配置是否正确，服务是否正常");
        }
        int size = queryRunners.size();
        int i = random.nextInt(size);
        DorisQueryRunner dorisQueryRunner = queryRunners.get(i);
        return dorisQueryRunner;
    }



    @Override
    public int[] batch(Connection conn, String sql, Object[][] params) throws SQLException {
        return getQueryRunner().batch(conn, sql, params);
    }

    @Override
    public int[] batch(String sql, Object[][] params) throws SQLException {
        return getQueryRunner().batch(sql, params);
    }


    @Override
    public <T> T query(String sql, Object param, ResultSetHandler<T> rsh) throws SQLException {
        return getQueryRunner().query(sql, param, rsh);
    }

    @Override
    public <T> T query(String sql, Object[] params, ResultSetHandler<T> rsh) throws SQLException {
        return getQueryRunner().query(sql, params, rsh);
    }

    @Override
    public <T> T query(String sql, ResultSetHandler<T> rsh, Object... params) throws SQLException {
        return getQueryRunner().query(sql, rsh, params);
    }

    @Override
    public <T> T query(String sql, ResultSetHandler<T> rsh) throws SQLException {
        return getQueryRunner().query(sql, rsh);
    }


    @Override
    public int update(String sql) throws SQLException {
        return getQueryRunner().update(sql);
    }

    @Override
    public int update(String sql, Object param) throws SQLException {
        return getQueryRunner().update(sql, param);
    }

    @Override
    public int update(String sql, Object... params) throws SQLException {
        return getQueryRunner().update(sql, params);
    }

    @Override
    public <T> T insert(String sql, ResultSetHandler<T> rsh) throws SQLException {
        return getQueryRunner().insert(sql, rsh);
    }

    @Override
    public <T> T insert(String sql, ResultSetHandler<T> rsh, Object... params) throws SQLException {
        return getQueryRunner().insert(sql, rsh, params);
    }

    @Override
    public <T> T insertBatch(String sql, ResultSetHandler<T> rsh, Object[][] params) throws SQLException {
        return getQueryRunner().insertBatch(sql, rsh, params);
    }


    @Override
    public int execute(String sql, Object... params) throws SQLException {
        return getQueryRunner().execute(sql, params);
    }

    @Override
    public <T> List<T> execute(String sql, ResultSetHandler<T> rsh, Object... params) throws SQLException {
        return getQueryRunner().execute(sql, rsh, params);
    }

    @Override
    public DataSource getDataSource() {
        return getQueryRunner().getDataSource();
    }

    @Override
    public boolean isPmdKnownBroken() {
        return getQueryRunner().isPmdKnownBroken();
    }

    /**
     * 通过 HTTP 方式 load 数据
     *
     * @param database
     * @param tableName
     * @param jsonData
     * @param groupCommit
     * @return
     */
    public boolean insertByStreamLoad(String database, String tableName, String jsonData, String groupCommit) throws Exception {
       return getQueryRunner().insertByStreamLoad(database, tableName, jsonData, groupCommit);
    }


    @Override
    public void close() throws Exception {
        flag = false;
        for (DorisQueryRunner queryRunner : queryRunners) {
            queryRunner.close();
        }
    }
}
