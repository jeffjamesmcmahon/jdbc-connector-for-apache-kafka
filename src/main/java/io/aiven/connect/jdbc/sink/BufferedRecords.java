/*
 * Copyright 2020 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.connect.jdbc.sink;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Timestamp;

import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.aiven.connect.jdbc.sink.metadata.FieldsMetadata;
import io.aiven.connect.jdbc.sink.metadata.SchemaPair;
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.TableDefinition;
import io.aiven.connect.jdbc.util.TableId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.connect.jdbc.sink.JdbcSinkConfig.InsertMode.*;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferedRecords {
    private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

    private final TableId tableId;
    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    private final Connection connection;

    private List<SinkRecord> records = new ArrayList<>();

    private int insertRecordCount = 0;
    private int deleteRecordCount = 0;
    private int updateRecordCount = 0;
    private int truncateRecordCount = 0;
    private int replicaRecordCount = 0;

    private SchemaPair currentSchemaPair;
    private FieldsMetadata fieldsMetadata;
    private TableDefinition tableDefinition;
    private PreparedStatement insertStatement;
    private StatementBinder insertBinder;
    private PreparedStatement updateStatement;
    private PreparedStatement deleteStatement;
    private StatementBinder updateBinder;
    private StatementBinder deleteBinder;
    private boolean deletesPresentInBatch = false;

    public BufferedRecords(
            final JdbcSinkConfig config,
            final TableId tableId,
            final DatabaseDialect dbDialect,
            final DbStructure dbStructure,
            final Connection connection
    ) {
        this.tableId = tableId;
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.connection = connection;
    }

    public List<SinkRecord> add(final SinkRecord record) throws SQLException {
        boolean isDelete = false, isUpdate = false, isInsert=false, isTruncate=false, isReplica=false;
        final Header recordOpHeader = record.headers().lastWithName("__op");
        if (!isNull(recordOpHeader)) {
            final Object val = recordOpHeader.value();
            isUpdate = val.equals("u");
            isDelete = val.equals("d");
            isInsert = val.equals("c");
            isReplica = val.equals("r");
            isTruncate = val.equals("t");
        }
        else throw new IllegalArgumentException("This connector requires a header called __op with the opcode");

        final List<SinkRecord> flushed = new ArrayList<>();
        final SchemaPair recordSchemaPair = new SchemaPair(
                record.keySchema(),
                record.valueSchema()
        );

        if (currentSchemaPair == null) {
            log.debug("Current schema is null - need to reinit");
            reInitialize(recordSchemaPair);
        }

        boolean schemaChanged = false;
        if (isDelete) {
            if (config.deleteEnabled) {
                deletesPresentInBatch = true;
            }
        } else if (currentSchemaPair.equals(recordSchemaPair)) {
            if (config.deleteEnabled && deletesPresentInBatch) {
                // so we know there is a delete record in this batch - and now we have an insert or update to handle
                // lets flush this batch to avoid delete -> insert collisions in the same batch
                log.debug("Adding an insert or update record - but there is already a delete so flushing {}", records.size());
                flushed.addAll(flush());
            }
            schemaChanged = false;
        } else {
            // legit schema change - we will flush existing records and add after - table schema will be updated
            schemaChanged = true;
        }

        if (schemaChanged) {
            // Each batch needs to have the same schema, so when the schema changes we should flush, reset
            // state and re-attempt the add
            log.info("Flushing buffer due to unequal schema pairs: "
                    + "current schemas: {}, next schemas: {}", currentSchemaPair, recordSchemaPair);
            flushed.addAll(flush());
            currentSchemaPair = null;
            reInitialize(recordSchemaPair);
            flushed.addAll(add(record));
        } else {
            // Same schema so follow happy path
            records.add(record);

            if (isInsert || isReplica) {
                insertRecordCount++;
            }
            else if (isDelete) {
                deleteRecordCount++;
            }
            else if (isUpdate) {
                updateRecordCount++;
            }
            else if (isTruncate) {
                truncateRecordCount++;
            }
            //else if(isReplica){
            //    replicaRecordCount++;
           // }
            else throw new IllegalArgumentException("Unable to identify the OpCode for a record-" + record.toString());

            if (records.size() >= config.batchSize) {
                log.info("Flushing buffered records after exceeding configured batch size {}.", config.batchSize);
                flushed.addAll(flush());
            }
        }

        // flag this batch as having delete Present - so that if a new insert comes in
        // then we should flush before next add
        if (isDelete && config.deleteEnabled) {
            deletesPresentInBatch = true;
        }
        return flushed;
    }

    public List<SinkRecord> flush() throws SQLException {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        prepareStatements();

        int index = 1;

        for (final SinkRecord record : records) {
            boolean isDelete = false, isUpdate = false, isInsert=false, isTruncate=false, isReplica=false;
            final Header recordOpHeader = record.headers().lastWithName("__op");
            if (!isNull(recordOpHeader)) {
                final Object val = recordOpHeader.value();
                isUpdate = val.equals("u");
                isDelete = val.equals("d");
                isInsert = val.equals("c");
                isReplica = val.equals("r");
                isTruncate = val.equals("t");
            }
            else throw new IllegalArgumentException("This connector requires a header called __op with the opcode");

            if (isDelete && !isNull(deleteBinder)) {
                deleteBinder.bindRecord(record);
            } else if (isUpdate) {
                updateBinder.bindRecord(record);
            } else {
                if (config.insertMode == MULTI) {
                    // All records are bound to the same prepared statement,
                    // so when binding fields for record N (N > 0)
                    // we need to start at the index where binding fields for record N - 1 stopped.
                    index = insertBinder.bindRecord(index, record);
                } else {
                    insertBinder.bindRecord(record);
                }
            }

        }
        log.info("Flushing records {}:I, {}:D, {}:U, {}:T, {}:R", insertRecordCount,
                deleteRecordCount, updateRecordCount, truncateRecordCount, replicaRecordCount);

        int totalUpdateCount = 0;
        boolean successNoInfo = false;

        if (insertRecordCount > 0) {
            final int[] insertCount = executeInsertBatch();
            // how many total updates, inserts and deletes were done?
            for (final int insertRowCount : insertCount) {
                if (insertRowCount == Statement.SUCCESS_NO_INFO) {
                    successNoInfo = true;
                    continue;
                }
                totalUpdateCount += insertRowCount;
            }
        }
        if (updateRecordCount > 0) {
            final int[] updateCount = executeUpdateBatch();

            for (final int updateRowCount : updateCount) {
                if (updateRowCount == Statement.SUCCESS_NO_INFO) {
                    successNoInfo = true;
                    continue;
                }
                totalUpdateCount += updateRowCount;
            }
        }
        if (deleteRecordCount > 0) {
            if (nonNull(deleteBinder)) {
                final int[] deleteCount = executeDeleteBatch();
                for (final int deleteRowCount : deleteCount) {
                    if (deleteRowCount == Statement.SUCCESS_NO_INFO) {
                        successNoInfo = true;
                        continue;
                    }
                    totalUpdateCount += deleteRowCount;
                }
            }
        }
        if (totalUpdateCount != records.size() && !successNoInfo) {
            //throw new ConnectException(String.format(
            log.warn("Update count {} did not sum up to total number of records inserted {}",
                    totalUpdateCount, records.size()
            );
        }
        if (successNoInfo) {
            log.info(
                    "{} records:{} , but no count of the number of rows it affected is available",
                    config.insertMode,
                    records.size()
            );
        }

        insertRecordCount = 0;
        deleteRecordCount = 0;
        updateRecordCount = 0;
        truncateRecordCount = 0;
        replicaRecordCount = 0;
        deletesPresentInBatch = false;

        final List<SinkRecord> flushedRecords = records;
        records = new ArrayList<>();
        return flushedRecords;
    }

    private void prepareStatements() throws SQLException {
        close();
        if (insertRecordCount > 0) {
            final String insertSql;
            log.debug("Generating query for insert mode {} and {} records", config.insertMode, records.size());
            if (config.insertMode == MULTI) {
                insertSql = getMultiInsertSql();
            } else {
                insertSql = getInsertSql();
            }
            insertStatement = connection.prepareStatement(insertSql);
            insertBinder = dbDialect.statementBinder(
                    insertStatement,
                    config.pkMode,
                    currentSchemaPair,
                    fieldsMetadata,
                    config.insertMode
            );
            log.debug("Prepared SQL {} for insert mode {}", insertSql, config.insertMode);
        }

        if (updateRecordCount > 0) {
            final String updateSql = getUpdateSql();
            updateStatement = connection.prepareStatement(updateSql);
            updateBinder = dbDialect.statementBinder(
                    updateStatement,
                    config.pkMode,
                    currentSchemaPair,
                    fieldsMetadata,
                    UPDATE
            );
        }

        if (config.deleteEnabled && deleteRecordCount > 0) {
            final String deleteSql = getDeleteSql();
            deleteStatement = connection.prepareStatement(deleteSql);
            deleteBinder = dbDialect.statementBinder(
                    deleteStatement,
                    config.pkMode,
                    currentSchemaPair,
                    fieldsMetadata,
                    INSERT
            );
        }
    }

    /**
     * Re-initialize everything that depends on the record schema
     */
    private void reInitialize(final SchemaPair schemaPair) throws SQLException {
        log.debug("ReInitializing - will update table schema if needed");

        currentSchemaPair = schemaPair;
        fieldsMetadata = FieldsMetadata.extract(
                tableId.tableName(),
                config.pkMode,
                config.pkFields,
                config.fieldsWhitelist,
                currentSchemaPair
        );

        dbStructure.createOrAmendIfNecessary(
                config,
                connection,
                tableId,
                fieldsMetadata
        );

        // TODO - find a better place and way to do this - also optionally trigger with Config
        TableDefinition def = dbDialect.describeTable(connection, tableId);
        log.debug("TableDef looks like {} before adding __row_updated_at", def);
        if(!def.columnNames().contains("__row_updated_at")) {
            String sql = String.format("alter table %s ADD COLUMN __row_updated_at TIMESTAMP DEFAULT GETDATE()", tableId);
            dbDialect.applyDdlStatements(connection, Collections.singletonList(sql));
        }

        tableDefinition = dbStructure.tableDefinitionFor(tableId, connection);
        log.debug("TableDef now looks like {}", tableDefinition);
    }

    private int[] executeInsertBatch() throws SQLException {
        if (config.insertMode == MULTI) {
            insertStatement.addBatch();
        }
        log.debug("Executing insert batch with insert mode {} - {}", config.insertMode, insertStatement.toString());
        int[] retVal;
        try {
            retVal = insertStatement.executeBatch();
        } catch (final Exception ex) {
            log.error("Message {}, statement {}", ex.getMessage(), insertStatement.toString());
            throw ex;
        }
        return retVal;
    }

    private int[] executeDeleteBatch() throws SQLException {
        log.debug("Executing delete batch - {}", deleteStatement.toString());
        int[] retVal;
        try {
            retVal = deleteStatement.executeBatch();
        } catch (final Exception ex) {
            log.error("Message {}, statement {}", ex.getMessage(), deleteStatement.toString());
            throw ex;
        }
        return retVal;
    }

    private int[] executeUpdateBatch() throws SQLException {
        log.debug("Executing update batch - {}", updateStatement.toString());
        int[] retVal;
        try {
            retVal = updateStatement.executeBatch();
        } catch(final Exception ex) {
            log.error("Message {}, statement {}", ex.getMessage(), updateStatement.toString());
            throw ex;
        }
        return retVal;
    }
    
    private void bindRecords() throws SQLException {
        log.debug("Binding {} buffered records", records.size());
        int index = 1;
        for (final SinkRecord record : records) {
            if (config.insertMode == MULTI) {
                // All records are bound to the same prepared statement,
                // so when binding fields for record N (N > 0)
                // we need to start at the index where binding fields for record N - 1 stopped.
                index = insertBinder.bindRecord(index, record);
            } else {
                insertBinder.bindRecord(record);
            }
        }
        log.debug("Done binding records.");
    }

    public void close() throws SQLException {

        if (insertStatement != null) {
            log.debug("Closing BufferedRecords with preparedStatement: {}", insertStatement);
            insertStatement.close();
            insertStatement = null;
        }

        if (deleteStatement != null) {
            log.debug("Closing BufferedRecords with preparedStatement: {}", deleteStatement);
            deleteStatement.close();
            deleteStatement = null;
        }

        if (updateStatement != null) {
            log.debug("Closing BufferedRecords with preparedStatement: {}", updateStatement);
            updateStatement.close();
            updateStatement = null;
        }
    }

    private String getMultiInsertSql() {
        if (config.insertMode != MULTI) {
            throw new ConnectException(String.format(
                    "Multi-row first insert SQL unsupported by insert mode %s",
                    config.insertMode
            ));
        }
        try {
            return dbDialect.buildMultiInsertStatement(
                    tableId,
                    //records.size(),
                    insertRecordCount,
                    asColumns(fieldsMetadata.keyFieldNames),
                    asColumns(fieldsMetadata.nonKeyFieldNames)
            );
        } catch (final UnsupportedOperationException e) {
            throw new ConnectException(String.format(
                    "Write to table '%s' in MULTI mode is not supported with the %s dialect.",
                    tableId,
                    dbDialect.name()
            ));
        }
    }

    private String getInsertSql() {
        switch (config.insertMode) {
            case INSERT:
                return dbDialect.buildInsertStatement(
                        tableId,
                        tableDefinition,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames)
                );
            case UPSERT:
                if (fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
                                    + " primary key configuration",
                            tableId
                    ));
                }
                try {
                    return dbDialect.buildUpsertQueryStatement(
                            tableId,
                            tableDefinition,
                            asColumns(fieldsMetadata.keyFieldNames),
                            asColumns(fieldsMetadata.nonKeyFieldNames)
                    );
                } catch (final UnsupportedOperationException e) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
                            tableId,
                            dbDialect.name()
                    ));
                }
            case UPDATE:
                return dbDialect.buildUpdateStatement(
                        tableId,
                        tableDefinition,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames)
                );
            default:
                throw new ConnectException("Invalid insert mode");
        }
    }


    private String getDeleteSql() {
        String sql = null;
        if (config.deleteEnabled) {
            switch (config.pkMode) {
                case RECORD_KEY:
                    if (fieldsMetadata.keyFieldNames.isEmpty()) {
                        throw new ConnectException("Require primary keys to support delete");
                    }
                    try {
                        sql = dbDialect.buildDeleteStatement(
                                tableId,
                                asColumns(fieldsMetadata.keyFieldNames)
                        );
                    } catch (final UnsupportedOperationException e) {
                        throw new ConnectException(String.format(
                                "Deletes to table '%s' are not supported with the %s dialect.",
                                tableId,
                                dbDialect.name()
                        ));
                    }
                    break;

                default:
                    throw new ConnectException("Deletes are only supported for pk.mode record_key");
            }
        }
        return sql;
    }

    private String getUpdateSql() {
        final String sql = dbDialect.buildUpdateStatement(
                tableId,
                tableDefinition,
                asColumns(fieldsMetadata.keyFieldNames),
                asColumns(fieldsMetadata.nonKeyFieldNames));
        return sql;
    }

    private Collection<ColumnId> asColumns(final Collection<String> names) {
        return names.stream()
                .map(name -> new ColumnId(tableId, name))
                .collect(Collectors.toList());
    }
}
