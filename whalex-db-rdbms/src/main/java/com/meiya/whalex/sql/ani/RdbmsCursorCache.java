package com.meiya.whalex.sql.ani;

import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.interior.db.search.in.Page;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author 蔡荣桂
 * @date 2022/10/11
 * @project whalex-data-driver
 */
@Slf4j
@Data
public class RdbmsCursorCache extends AbstractCursorCache<AniHandler> {

    private Connection connection;

    private PreparedStatement preparedStatement;

    private ResultSet resultSet;

    public RdbmsCursorCache(Page lastPage, Integer batchSize, Connection connection, PreparedStatement preparedStatement, ResultSet resultSet) {
        super(lastPage, batchSize);
        this.connection = connection;
        this.preparedStatement = preparedStatement;
        this.resultSet = resultSet;
    }

    @Override
    public void closeCursor() {
        try {
            connection.commit();
        } catch (SQLException e) {
            log.error("执行事物提交失败!", e);
        }
        try {
            resultSet.close();
        } catch (SQLException e) {
            log.error("ResultSet 对象关闭失败!", e);
        }

        try {
            preparedStatement.close();
        } catch (SQLException e) {
            log.error("PreparedStatement 对象关闭失败!", e);

        }

        try {
            connection.close();
        } catch (SQLException e) {
            log.error("conn 对象关闭失败!", e);
        }
    }
}
