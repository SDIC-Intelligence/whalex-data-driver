package com.meiya.whalex.db.module.graph;

import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.DataResult;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.graph.NebulaGraphDatabaseInfo;
import com.meiya.whalex.db.entity.graph.NebulaGraphHandler;
import com.meiya.whalex.db.entity.graph.NebulaGraphTableInfo;
import com.meiya.whalex.db.entity.graph.rawstatement.NebulaGraphRecordImpl;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphRecord;
import com.meiya.whalex.graph.module.AbstractGraphModuleBaseService;
import com.vesoft.nebula.client.graph.SessionPool;
import com.vesoft.nebula.client.graph.data.ResultSet;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 黄河森
 * @date 2024/3/11
 * @package com.meiya.whalex.db.module.graph
 * @project whalex-data-driver
 * @description NebulaGraphRawStatementModuleImpl
 */
public abstract class NebulaGraphRawStatementModuleImpl<S extends SessionPool,
        Q extends NebulaGraphHandler,
        D extends NebulaGraphDatabaseInfo,
        T extends NebulaGraphTableInfo,
        C extends AbstractCursorCache> extends AbstractGraphModuleBaseService<S, Q, D, T, C> {

    @Override
    public DataResult<List<NebulaGraphRecord>> rawStatementExecute(DatabaseSetting databaseSetting, String rawStatement) throws Exception {
        return _execute(databaseSetting, (D dataConf) -> (String) rawStatement, this::execute);
    }

    public <R, P> R execute(NebulaGraphDatabaseInfo dataConf, SessionPool dbConnect, P params) throws Exception {
        ResultSet execute = dbConnect.execute((String) params);
        DataResult<List<NebulaGraphRecord>> dataResult = new DataResult<>();
        if (execute.isSucceeded()) {
            int size = execute.rowsSize();
            List<NebulaGraphRecord> records = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                ResultSet.Record record = execute.rowValues(i);
                records.add(new NebulaGraphRecordImpl(record));
            }
            dataResult.setData(records);
            dataResult.setTotal(size);
        } else {
            String errorMessage = execute.getErrorMessage();
            dataResult.setSuccess(false);
            dataResult.setMessage(errorMessage);
        }
        return (R) dataResult;
    }
}
