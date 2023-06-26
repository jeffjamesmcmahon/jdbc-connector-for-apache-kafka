/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.dialect.DatabaseDialects;
import io.aiven.connect.jdbc.sink.metadata.FieldsMetadata;
import io.aiven.connect.jdbc.util.TableId;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static java.sql.Statement.SUCCESS_NO_INFO;
import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BufferedRecordsTest {

    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());
    private final String dbUrl = sqliteHelper.sqliteUri();
    private Map<Object, Object> props;
    @Before
    public void setUp() throws IOException, SQLException {
        sqliteHelper.setUp();
        props = new HashMap<>();
        props.put("name", "my-connector");
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
        // We don't manually create the table, so let the connector do it
        props.put("auto.create", true);
        // We use various schemas, so let the connector add missing columns
        props.put("auto.evolve", true);
    }

    @After
    public void tearDown() throws IOException, SQLException {
        sqliteHelper.tearDown();
    }

    @Test
    public void correctUpdateBatching() throws SQLException {
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(dbUrl, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new BufferedRecords(
                config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        final Schema schemaA = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct valueA = new Struct(schemaA)
                .put("name", "cuba");
        final SinkRecord recordA = wrapInSinkRecord(valueA);

        final Schema schemaB = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        final Struct valueB = new Struct(schemaB)
                .put("name", "cuba")
                .put("age", 4);
        final SinkRecord recordB = new SinkRecord("dummy", 1, null, null, schemaB, valueB, 1);
        recordB.headers().addString("__op", "c");

        // test records are batched correctly based on schema equality as records are added
        //   (schemaA,schemaA,schemaA,schemaB,schemaA) -> ([schemaA,schemaA,schemaA],[schemaB],[schemaA])

        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));

        assertEquals(Arrays.asList(recordA, recordA, recordA), buffer.add(recordB));

        assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

        assertEquals(Collections.singletonList(recordA), buffer.flush());
    }
    @Test
    public void correctBatching() throws SQLException {
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(dbUrl, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new BufferedRecords(
                config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        final Schema schemaA = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct valueA = new Struct(schemaA)
                .put("name", "cuba");
        final SinkRecord recordA = wrapInSinkRecord(valueA);
        final Schema schemaB = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        final Struct valueB = new Struct(schemaB)
                .put("name", "cuba")
                .put("age", 4);
        final SinkRecord recordB = new SinkRecord("dummy", 1, null, null, schemaB, valueB, 1);
        recordB.headers().addString("__op", "c");
        // test records are batched correctly based on schema equality as records are added
        //   (schemaA,schemaA,schemaA,schemaB,schemaA) -> ([schemaA,schemaA,schemaA],[schemaB],[schemaA])

        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));

        assertEquals(Arrays.asList(recordA, recordA, recordA), buffer.add(recordB));

        assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

        assertEquals(Collections.singletonList(recordA), buffer.flush());
    }

    @Test
    public void insertThenDeleteInBatchNoFlush() throws SQLException {
        props.put("delete.enabled", true);
        props.put("insert.mode", "upsert");
        props.put("pk.mode", "record_key");
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        final Schema keySchemaA = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .build();
        final Schema valueSchemaA = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct keyA = new Struct(keySchemaA)
                .put("id", 1234L);
        final Struct valueA = new Struct(valueSchemaA)
                .put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
        recordA.headers().addString("__op", "c");
        final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyA, null, null, 0);
        recordADelete.headers().addString("__op", "d");

        final Schema schemaB = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        final Struct valueB = new Struct(schemaB)
                .put("name", "cuba")
                .put("age", 4);
        final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);
        recordB.headers().addString("__op", "c");
        // test records are batched correctly based on schema equality as records are added
        //   (schemaA,schemaA,schemaA,schemaB,schemaA) -> ([schemaA,schemaA,schemaA],[schemaB],[schemaA])

        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));

        // delete should not cause a flush (i.e. not treated as a schema change)
        assertEquals(Collections.emptyList(), buffer.add(recordADelete));

        // schema change should trigger flush
        assertEquals(Arrays.asList(recordA, recordA, recordADelete), buffer.add(recordB));

        // second schema change should trigger flush
        assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

        assertEquals(Collections.singletonList(recordA), buffer.flush());
    }

    @Test
    public void insertThenTwoDeletesWithSchemaInBatchNoFlush() throws SQLException {
        props.put("delete.enabled", true);
        props.put("insert.mode", "upsert");
        props.put("pk.mode", "record_key");
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        final Schema keySchemaA = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .build();
        final Schema valueSchemaA = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct keyA = new Struct(keySchemaA)
                .put("id", 1234L);
        final Struct keyB = new Struct(keySchemaA)
                .put("id", 4321L);
        final Struct valueA = new Struct(valueSchemaA)
                .put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
        recordA.headers().addString("__op", "c");
        final SinkRecord recordADeleteWithSchema = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, null, 0);
        recordADeleteWithSchema.headers().addString("__op", "d");
        final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyB, null, null, 0);
        recordADelete.headers().addString("__op", "d");

        final Schema schemaB = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        final Struct valueB = new Struct(schemaB)
                .put("name", "cuba")
                .put("age", 4);
        final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);
        recordB.headers().addString("__op", "c");
        // test records are batched correctly based on schema equality as records are added
        //   (schemaA,schemaA,schemaA,schemaB,schemaA) -> ([schemaA,schemaA,schemaA],[schemaB],[schemaA])

        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));

        // delete should not cause a flush (i.e. not treated as a schema change)
        assertEquals(Collections.emptyList(), buffer.add(recordADeleteWithSchema));

        // delete should not cause a flush (i.e. not treated as a schema change)
        assertEquals(Collections.emptyList(), buffer.add(recordADelete));

        // schema change and/or previous deletes should trigger flush
        assertEquals(Arrays.asList(recordA, recordA, recordADeleteWithSchema, recordADelete), buffer.add(recordB));

        // second schema change should trigger flush
        assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

        assertEquals(Collections.singletonList(recordA), buffer.flush());
    }

    @Test
    public void insertThenDeleteThenInsertInBatchFlush() throws SQLException {
        props.put("delete.enabled", true);
        props.put("insert.mode", "upsert");
        props.put("pk.mode", "record_key");
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        final Schema keySchemaA = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .build();
        final Schema valueSchemaA = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct keyA = new Struct(keySchemaA)
                .put("id", 1234L);
        final Struct valueA = new Struct(valueSchemaA)
                .put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
        recordA.headers().addString("__op", "c");
        final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyA, null, null, 0);
        recordADelete.headers().addString("__op", "d");
        final Schema schemaB = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        final Struct valueB = new Struct(schemaB)
                .put("name", "cuba")
                .put("age", 4);
        final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);
        recordB.headers().addString("__op", "c");
        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));

        // delete should not cause a flush (i.e. not treated as a schema change)
        assertEquals(Collections.emptyList(), buffer.add(recordADelete));

        // insert after delete should flush to insure insert isn't lost in batching
        assertEquals(Arrays.asList(recordA, recordA, recordADelete), buffer.add(recordA));

        // schema change should trigger flush
        assertEquals(Collections.singletonList(recordA), buffer.add(recordB));

        // second schema change should trigger flush
        assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

        assertEquals(Collections.singletonList(recordA), buffer.flush());
    }

    @Test
    public void insertThenDeleteWithSchemaThenInsertInBatchFlush() throws SQLException {
        props.put("delete.enabled", true);
        props.put("insert.mode", "upsert");
        props.put("pk.mode", "record_key");
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        final Schema keySchemaA = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .build();
        final Schema valueSchemaA = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct keyA = new Struct(keySchemaA)
                .put("id", 1234L);
        final Struct valueA = new Struct(valueSchemaA)
                .put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
        recordA.headers().addString("__op", "c");
        final SinkRecord recordADeleteWithSchema = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, null, 0);
        recordADeleteWithSchema.headers().addString("__op", "d");
        final Schema schemaB = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        final Struct valueB = new Struct(schemaB)
                .put("name", "cuba")
                .put("age", 4);
        final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);
        recordB.headers().addString("__op", "c");
        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));

        // delete should not cause a flush (i.e. not treated as a schema change)
        assertEquals(Collections.emptyList(), buffer.add(recordADeleteWithSchema));

        // insert after delete should flush to insure insert isn't lost in batching
        assertEquals(Arrays.asList(recordA, recordA, recordADeleteWithSchema), buffer.add(recordA));

        // schema change should trigger flush
        assertEquals(Collections.singletonList(recordA), buffer.add(recordB));

        // second schema change should trigger flush
        assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

        assertEquals(Collections.singletonList(recordA), buffer.flush());
    }

    @Test
    public void testMultipleDeletesBatchedTogether() throws SQLException {
        props.put("delete.enabled", true);
        props.put("insert.mode", "upsert");
        props.put("pk.mode", "record_key");
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        final Schema keySchemaA = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .build();
        final Schema valueSchemaA = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct keyA = new Struct(keySchemaA)
                .put("id", 1234L);
        final Struct valueA = new Struct(valueSchemaA)
                .put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
        recordA.headers().addString("__op", "c");

        final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyA, null, null, 0);
        recordADelete.headers().addString("__op", "d");

        final Schema schemaB = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        final Struct valueB = new Struct(schemaB)
                .put("name", "cuba")
                .put("age", 4);
        final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);
        recordB.headers().addString("__op", "c");
        final SinkRecord recordBDelete = new SinkRecord("dummy", 1, keySchemaA, keyA, null, null, 1);
        recordBDelete.headers().addString("__op", "d");

        assertEquals(Collections.emptyList(), buffer.add(recordA));

        // schema change should trigger flush
        assertEquals(Collections.singletonList(recordA), buffer.add(recordB));

        // deletes should not cause a flush (i.e. not treated as a schema change)
        assertEquals(Collections.emptyList(), buffer.add(recordADelete));
        assertEquals(Collections.emptyList(), buffer.add(recordBDelete));

        // insert after delete should flush to insure insert isn't lost in batching
        assertEquals(Arrays.asList(recordB, recordADelete, recordBDelete), buffer.add(recordB));

        assertEquals(Collections.singletonList(recordB), buffer.flush());
    }

    @Test
    public void testMultipleDeletesWithSchemaBatchedTogether() throws SQLException {
        props.put("delete.enabled", true);
        props.put("insert.mode", "upsert");
        props.put("pk.mode", "record_key");
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        final Schema keySchemaA = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .build();
        final Schema valueSchemaA = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct keyA = new Struct(keySchemaA)
                .put("id", 1234L);
        final Struct valueA = new Struct(valueSchemaA)
                .put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
        recordA.headers().addString("__op", "c");
        final SinkRecord recordADeleteWithSchema = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, null, 0);
        recordADeleteWithSchema.headers().addString("__op", "d");

        final Schema schemaB = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        final Struct valueB = new Struct(schemaB)
                .put("name", "cuba")
                .put("age", 4);

        final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);
        recordB.headers().addString("__op", "c");
        final SinkRecord recordBDeleteWithSchema = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, null, 1);
        recordBDeleteWithSchema.headers().addString("__op", "d");
        assertEquals(Collections.emptyList(), buffer.add(recordA));

        // schema change should trigger flush
        assertEquals(Collections.singletonList(recordA), buffer.add(recordB));

        // schema change should trigger flush
        assertEquals(Collections.singletonList(recordB), buffer.add(recordADeleteWithSchema));

        // schema change should trigger flush
        assertEquals(Collections.singletonList(recordADeleteWithSchema), buffer.add(recordBDeleteWithSchema));

        // insert after delete should flush to insure insert isn't lost in batching
        assertEquals(Collections.singletonList(recordBDeleteWithSchema), buffer.add(recordB));

        assertEquals(Collections.singletonList(recordB), buffer.flush());
    }

    @Test(expected = ConfigException.class)
    public void configParsingFailsIfDeleteWithWrongPKMode() {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("delete.enabled", true);
        props.put("insert.mode", "upsert");
        props.put("pk.mode", "kafka"); // wrong pk mode for deletes
        new JdbcSinkConfig(props);
    }

    @Test
    public void testFlushSuccessNoInfo() throws SQLException {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("connection.url", "");
        props.put("auto.create", true);
        props.put("auto.evolve", true);
        props.put("batch.size", 1000);
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(dbUrl, config);

        final int[] batchResponse = new int[] {SUCCESS_NO_INFO, SUCCESS_NO_INFO};

        final DbStructure dbStructureMock = mock(DbStructure.class);
        when(dbStructureMock.createOrAmendIfNecessary(any(JdbcSinkConfig.class),
                any(Connection.class),
                any(TableId.class),
                any(FieldsMetadata.class)))
                .thenReturn(true);

        final PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
        when(preparedStatementMock.executeBatch()).thenReturn(batchResponse);

        final Connection connectionMock = mock(Connection.class);
        when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect,
                dbStructureMock, connectionMock);

        final Schema schemaA = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
        final Struct valueA = new Struct(schemaA).put("name", "cuba");
        final SinkRecord recordA = wrapInSinkRecord(valueA);
        buffer.add(recordA);

        final Schema schemaB = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
        final Struct valueB = new Struct(schemaA).put("name", "cubb");
        final SinkRecord recordB = wrapInSinkRecord(valueB);
        buffer.add(recordB);
        buffer.flush();

    }


    @Test
    public void testInsertModeMultiAutomaticFlush() throws SQLException {
        final JdbcSinkConfig config = multiModeConfig(2);

        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(dbUrl, config);
        final DbStructure dbStructureMock = mock(DbStructure.class);
        when(dbStructureMock.createOrAmendIfNecessary(any(JdbcSinkConfig.class),
                any(Connection.class),
                any(TableId.class),
                any(FieldsMetadata.class)))
                .thenReturn(true);

        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeBatch()).thenReturn(new int[]{2});

        final TableId tableId = new TableId(null, null, "planets");
        final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructureMock,
                connection);

        final Schema schema = newPlanetSchema();
        for (int i = 1; i <= 5; i++) {
            buffer.add(wrapInSinkRecord(newPlanet(schema, 1, "planet name " + i)));
        }

        final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        // Given the 5 records, and batch size of 2, we expect 2 inserts.
        // One record is still waiting in the buffer, and that is expected.
        verify(connection, times(2)).prepareStatement(sqlCaptor.capture());
        assertEquals(
                sqlCaptor.getAllValues().get(0),
                "INSERT INTO \"planets\"(\"name\",\"planetid\") VALUES (?,?),(?,?)"
        );
        assertEquals(
                sqlCaptor.getAllValues().get(1),
                "INSERT INTO \"planets\"(\"name\",\"planetid\") VALUES (?,?),(?,?)"
        );

    }

    @Test
    public void testInsertModeMultiExplicitFlush() throws SQLException {
        final JdbcSinkConfig config = multiModeConfig(100);

        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(dbUrl, config);
        final DbStructure dbStructureMock = mock(DbStructure.class);
        when(dbStructureMock.createOrAmendIfNecessary(any(JdbcSinkConfig.class),
                any(Connection.class),
                any(TableId.class),
                any(FieldsMetadata.class)))
                .thenReturn(true);

        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeBatch()).thenReturn(new int[]{2});

        final TableId tableId = new TableId(null, null, "planets");
        final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructureMock,
                connection);

        final Schema schema = newPlanetSchema();
        final Struct valueA = newPlanet(schema, 1, "mercury");
        final Struct valueB = newPlanet(schema, 2, "venus");
        buffer.add(wrapInSinkRecord(valueA));
        buffer.add(wrapInSinkRecord(valueB));
        buffer.flush();

        verify(connection).prepareStatement(
                "INSERT INTO \"planets\"(\"name\",\"planetid\") VALUES (?,?),(?,?)"
        );

    }


    @Test
    public void testAddRecordDeleteEnabledAndNonePkMode() throws SQLException {
        props.put("delete.enabled", true);
        props.put("pk.mode", "none");
        ConfigException e = assertThrows(ConfigException.class, () -> new JdbcSinkConfig(props));
        assertEquals(
                "Primary key mode must be 'record_key' when delete support is enabled",
                e.getMessage()
        );
    }

    @Test
    public void testAddRecordDeleteEnabledAndRecordValuePkMode() throws SQLException {
        props.put("delete.enabled", true);
        props.put("pk.mode", "record_value");
        props.put("pk.fields", "name");
        ConfigException e = assertThrows(ConfigException.class, () -> new JdbcSinkConfig(props));
        assertEquals(
                "Primary key mode must be 'record_key' when delete support is enabled",
                e.getMessage()
        );
    }

    @Test
    public void testAddRecordDeleteEnabledAndKafkaPkMode() throws SQLException {
        props.put("delete.enabled", true);
        props.put("pk.mode", "kafka");
        ConfigException e = assertThrows(ConfigException.class, () -> new JdbcSinkConfig(props));
        assertEquals(
                "Primary key mode must be 'record_key' when delete support is enabled",
                e.getMessage()
        );
    }

    private Struct newPlanet(final Schema schema, final int id, final String name) {
        return new Struct(schema)
                .put("planetid", id)
                .put("name", name);
    }

    private Schema newPlanetSchema() {
        return SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("planetid", Schema.INT32_SCHEMA)
                .build();
    }

    private JdbcSinkConfig multiModeConfig(final int batchSize) {
        return new JdbcSinkConfig(Map.of(
                "connection.url", "",
                "auto.create", true,
                "auto.evolve", true,
                "batch.size", batchSize,
                "insert.mode", "multi"
        ));
    }

    private SinkRecord wrapInSinkRecord(final Struct value) {
        SinkRecord retVal = new SinkRecord("dummy-topic", 0, null, null, value.schema(), value, 0);
        retVal.headers().addString("__op", "c");
        return retVal;
    }

    protected SinkRecord generateRecord(
            boolean includeKeySchema,
            boolean includeKey,
            boolean includeValueSchema,
            boolean includeValue
    ) {
        Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .build();
        Schema valueSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();
        Schema keySchemaForRecord = includeKeySchema ? keySchema : null;
        Schema valueSchemaForRecord = includeValueSchema ? valueSchema : null;
        final Object key = includeKey ? new Struct(keySchema).put("id", 100) : null;
        final Object valueA = includeValue ? new Struct(valueSchema).put("name", "cuba") : null;
        return new SinkRecord("dummy", 0, keySchemaForRecord, key, valueSchemaForRecord, valueA, 0);
    }
    protected void assertInvalidRecord(
            boolean includeKeySchema,
            boolean includeKey,
            boolean includeValueSchema,
            boolean includeValue,
            String errorMessageFragment
    ) {
        assertInvalidRecord(
                generateRecord(includeKeySchema, includeKey, includeValueSchema, includeValue),
                errorMessageFragment
        );
    }

    protected void assertInvalidRecord(SinkRecord record, String errorMessageFragment) {
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        ConnectException e = assertThrows(ConnectException.class, () -> {
            buffer.add(record);
            buffer.flush();
        });
        assertTrue(
                "Unexpected message: " + e.getMessage(),
                e.getMessage().contains(errorMessageFragment)
        );
    }

    protected void assertValidRecord(
            boolean includeKeySchema,
            boolean includeKey,
            boolean includeValueSchema,
            boolean includeValue
    ) throws SQLException {
        assertValidRecord(
                generateRecord(includeKeySchema, includeKey, includeValueSchema, includeValue)
        );
    }

    protected void assertValidRecord(SinkRecord record) throws SQLException {
        props.put("batch.size", 2);
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        List<SinkRecord> flushed = buffer.add(record);
        assertEquals(Collections.emptyList(), flushed);
    }
}
