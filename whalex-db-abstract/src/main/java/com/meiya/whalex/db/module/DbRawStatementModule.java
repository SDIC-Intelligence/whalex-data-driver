package com.meiya.whalex.db.module;

import com.meiya.whalex.db.entity.DataResult;
import com.meiya.whalex.db.entity.DatabaseSetting;

public interface DbRawStatementModule {

    DataResult rawStatementExecute(DatabaseSetting databaseSetting, String rawStatement) throws Exception;
}
