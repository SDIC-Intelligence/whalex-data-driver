package com.meiya.whalex.jdbc;

import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.List;

@Slf4j
public class DatDatabaseMetaData implements DatabaseMetaData {
    
    private String url;
    
    private String userName;

    //支持能力的列表
    private List<SupportPower> supportPowerList;

    private DataSource dataSource;

    private DbResourceEnum dbType;

    public DatDatabaseMetaData(String url, String userName, List<SupportPower> supportList, DbResourceEnum dbType) {
        
        this.url = url;
        this.userName = userName;
        this.supportPowerList = supportList;
        this.dbType = dbType;
    }

    public DatDatabaseMetaData(String url, String userName, List<SupportPower> supportList, DataSource dataSource, DbResourceEnum dbType) {
        this(url, userName, supportList, dbType);
        this.dataSource = dataSource;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().allProceduresAreCallable();
            }
        }
        log.error(this.getClass().getName() + "#" + "allProceduresAreCallable 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().allTablesAreSelectable();
            }
        }
        log.error(this.getClass().getName() + "#" + "allTablesAreSelectable 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getURL() throws SQLException {
        return url;
    }

    @Override
    public String getUserName() throws SQLException {
        return userName;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        log.error(this.getClass().getName() + "#" + "nullsAreSortedHigh 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        log.error(this.getClass().getName() + "#" + "nullsAreSortedLow 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        log.error(this.getClass().getName() + "#" + "nullsAreSortedAtStart 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        log.error(this.getClass().getName() + "#" + "nullsAreSortedAtEnd 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        if(dbType == DbResourceEnum.es || dbType == DbResourceEnum.mongodb) {
            return "qkOS noSql";
        }
        return "qkOS";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "1.0";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "DatSQL Connector";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "DatSQL 1.0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 1;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        log.error(this.getClass().getName() + "#" + "usesLocalFiles 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        log.error(this.getClass().getName() + "#" + "usesLocalFilePerTable 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        log.error(this.getClass().getName() + "#" + "storesUpperCaseIdentifiers 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        log.error(this.getClass().getName() + "#" + "storesLowerCaseIdentifiers 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        log.error(this.getClass().getName() + "#" + "storesMixedCaseIdentifiers 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        log.error(this.getClass().getName() + "#" + "storesUpperCaseQuotedIdentifiers 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        log.error(this.getClass().getName() + "#" + "storesLowerCaseQuotedIdentifiers 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        log.error(this.getClass().getName() + "#" + "storesMixedCaseQuotedIdentifiers 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getIdentifierQuoteString 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getSQLKeywords 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getNumericFunctions 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getStringFunctions() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getStringFunctions 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getSystemFunctions 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getTimeDateFunctions 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getSearchStringEscape 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getExtraNameCharacters 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return supportPowerList.contains(SupportPower.MODIFY_TABLE);
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return supportPowerList.contains(SupportPower.MODIFY_TABLE);
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        //支持给列起别名
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        log.error(this.getClass().getName() + "#" + "nullPlusNonNullIsNull 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return supportPowerList.contains(SupportPower.TRANSACTION);
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsNonNullableColumns();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsNonNullableColumns 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsMinimumSQLGrammar();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsMinimumSQLGrammar 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsCoreSQLGrammar();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsCoreSQLGrammar 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsExtendedSQLGrammar();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsExtendedSQLGrammar 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsANSI92EntryLevelSQL();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsANSI92EntryLevelSQL 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsANSI92IntermediateSQL();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsANSI92IntermediateSQL 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsANSI92FullSQL();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsANSI92FullSQL 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsIntegrityEnhancementFacility();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsIntegrityEnhancementFacility 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsOuterJoins();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsOuterJoins 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsFullOuterJoins();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsFullOuterJoins 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsLimitedOuterJoins();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsLimitedOuterJoins 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getSchemaTerm 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getProcedureTerm 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getCatalogTerm 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        log.error(this.getClass().getName() + "#" + "isCatalogAtStart 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getCatalogSeparator 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsSchemasInDataManipulation();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsSchemasInDataManipulation 方法未实现");
        throw new SQLException("supportsSchemasInDataManipulation方法未实现");
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsSchemasInProcedureCalls();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsSchemasInProcedureCalls 方法未实现");
        throw new SQLException("supportsSchemasInProcedureCalls方法未实现");
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsSchemasInTableDefinitions();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsSchemasInTableDefinitions 方法未实现");
        throw new SQLException("supportsSchemasInTableDefinitions方法未实现");
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsSchemasInIndexDefinitions();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsSchemasInIndexDefinitions 方法未实现");
        throw new SQLException("supportsSchemasInIndexDefinitions方法未实现");
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsSchemasInPrivilegeDefinitions();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsSchemasInPrivilegeDefinitions 方法未实现");
        throw new SQLException("supportsSchemasInPrivilegeDefinitions方法未实现");
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsCatalogsInDataManipulation();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsCatalogsInDataManipulation 方法未实现");
        throw new SQLException("supportsCatalogsInDataManipulation方法未实现");
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsCatalogsInProcedureCalls();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsCatalogsInProcedureCalls 方法未实现");
        throw new SQLException("supportsCatalogsInProcedureCalls方法未实现");
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsCatalogsInTableDefinitions();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsCatalogsInTableDefinitions 方法未实现");
        throw new SQLException("supportsCatalogsInTableDefinitions方法未实现");
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsCatalogsInIndexDefinitions();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsCatalogsInIndexDefinitions 方法未实现");
        throw new SQLException("supportsCatalogsInIndexDefinitions方法未实现");
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsCatalogsInPrivilegeDefinitions();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsCatalogsInPrivilegeDefinitions 方法未实现");
        throw new SQLException("supportsCatalogsInPrivilegeDefinitions方法未实现");
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsPositionedDelete();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsPositionedDelete 方法未实现");
        throw new SQLException("supportsPositionedDelete方法未实现");
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsPositionedUpdate();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsPositionedUpdate 方法未实现");
        throw new SQLException("supportsPositionedUpdate方法未实现");
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsSelectForUpdate();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsSelectForUpdate 方法未实现");
        throw new SQLException("supportsSelectForUpdate方法未实现");
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsStoredProcedures();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsStoredProcedures 方法未实现");
        throw new SQLException("supportsStoredProcedures方法未实现");
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsSubqueriesInComparisons();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsSubqueriesInComparisons 方法未实现");
        throw new SQLException("supportsSubqueriesInComparisons方法未实现");
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsSubqueriesInExists();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsSubqueriesInExists 方法未实现");
        throw new SQLException("supportsSubqueriesInExists方法未实现");
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsSubqueriesInIns();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsSubqueriesInIns 方法未实现");
        throw new SQLException("supportsSubqueriesInIns方法未实现");
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsSubqueriesInQuantifieds();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsSubqueriesInQuantifieds 方法未实现");
        throw new SQLException("supportsSubqueriesInQuantifieds方法未实现");
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsCorrelatedSubqueries();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsCorrelatedSubqueries 方法未实现");
        throw new SQLException("supportsCorrelatedSubqueries方法未实现");
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsUnion();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsUnion 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsUnionAll();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsUnionAll 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsOpenCursorsAcrossCommit();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsOpenCursorsAcrossCommit 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsOpenCursorsAcrossRollback();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsOpenCursorsAcrossRollback 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsOpenStatementsAcrossCommit();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsOpenStatementsAcrossCommit 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsOpenStatementsAcrossRollback();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsOpenStatementsAcrossRollback 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxBinaryLiteralLength 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxCharLiteralLength 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxColumnNameLength 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxColumnsInGroupBy 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxColumnsInIndex 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxColumnsInOrderBy 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxColumnsInSelect 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxColumnsInTable 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxConnections() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxConnections 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxCursorNameLength 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxIndexLength 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxSchemaNameLength 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxProcedureNameLength 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxCatalogNameLength 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxRowSize 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        log.error(this.getClass().getName() + "#" + "doesMaxRowSizeIncludeBlobs 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxStatementLength 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxStatements() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxStatements 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxTableNameLength 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxTablesInSelect 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getMaxUserNameLength 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getDefaultTransactionIsolation 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsTransactions();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsTransactions 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsTransactionIsolationLevel(level);
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsTransactionIsolationLevel 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsDataDefinitionAndDataManipulationTransactions();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsDataDefinitionAndDataManipulationTransactions 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsDataManipulationTransactionsOnly();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsDataManipulationTransactionsOnly 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        log.error(this.getClass().getName() + "#" + "dataDefinitionCausesTransactionCommit 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        log.error(this.getClass().getName() + "#" + "dataDefinitionIgnoredInTransactions 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getProcedures 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getProcedureColumns 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getTables 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getSchemas 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getCatalogs 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getTableTypes 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getColumns 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getColumnPrivileges 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getTablePrivileges 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getBestRowIdentifier 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getVersionColumns 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getPrimaryKeys 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getImportedKeys 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getExportedKeys 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getCrossReference 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getTypeInfo 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getIndexInfo 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsResultSetType(type);
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsResultSetType 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsResultSetConcurrency(type, concurrency);
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsResultSetConcurrency 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        log.error(this.getClass().getName() + "#" + "ownUpdatesAreVisible 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        log.error(this.getClass().getName() + "#" + "ownDeletesAreVisible 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        log.error(this.getClass().getName() + "#" + "ownInsertsAreVisible 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        log.error(this.getClass().getName() + "#" + "othersUpdatesAreVisible 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        log.error(this.getClass().getName() + "#" + "othersDeletesAreVisible 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        log.error(this.getClass().getName() + "#" + "othersInsertsAreVisible 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updatesAreDetected 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        log.error(this.getClass().getName() + "#" + "deletesAreDetected 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        log.error(this.getClass().getName() + "#" + "insertsAreDetected 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsBatchUpdates();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsBatchUpdates 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getUDTs 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Connection getConnection() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getConnection 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsSavepoints();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsSavepoints 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsNamedParameters();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsNamedParameters 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsMultipleOpenResults();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsMultipleOpenResults 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsGetGeneratedKeys();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsGetGeneratedKeys 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getSuperTypes 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getSuperTables 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getAttributes 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsResultSetHoldability(holdability);
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsResultSetHoldability 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getResultSetHoldability 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getSQLStateType 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        log.error(this.getClass().getName() + "#" + "locatorsUpdateCopy 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsStatementPooling();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsStatementPooling 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getRowIdLifetime 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getSchemas 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        if(dataSource != null) {
            try(Connection connection = dataSource.getConnection()) {
                return connection.getMetaData().supportsStoredFunctionsUsingCallSyntax();
            }
        }
        log.error(this.getClass().getName() + "#" + "supportsStoredFunctionsUsingCallSyntax 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        log.error(this.getClass().getName() + "#" + "autoCommitFailureClosesAllResultSets 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getClientInfoProperties 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getFunctions 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getFunctionColumns 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getPseudoColumns 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        log.error(this.getClass().getName() + "#" + "generatedKeyAlwaysReturned 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        log.error(this.getClass().getName() + "#" + "unwrap 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        log.error(this.getClass().getName() + "#" + "isWrapperFor 方法未实现");
        throw new SQLException("方法未实现");
    }
}
