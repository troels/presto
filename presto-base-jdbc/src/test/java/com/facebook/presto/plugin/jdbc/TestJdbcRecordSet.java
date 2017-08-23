/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test
public class TestJdbcRecordSet
{
    private TestingDatabase database;
    private JdbcClient jdbcClient;
    private JdbcSplit split;
    private Map<String, JdbcColumnHandle> columnHandles;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        jdbcClient = database.getJdbcClient();
        split = database.getSplit("example", "numbers");
        columnHandles = database.getColumnHandles("example", "numbers");
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        database.close();
    }

    @Test
    public void testGetColumnTypes()
            throws Exception
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of(
                new JdbcColumnHandle("test", "text", VARCHAR),
                new JdbcColumnHandle("test", "text_short", createVarcharType(32)),
                new JdbcColumnHandle("test", "value", BIGINT)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(VARCHAR, createVarcharType(32), BIGINT));

        recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of(
                new JdbcColumnHandle("test", "value", BIGINT),
                new JdbcColumnHandle("test", "text", VARCHAR)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, VARCHAR));

        recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of(
                new JdbcColumnHandle("test", "value", BIGINT),
                new JdbcColumnHandle("test", "value", BIGINT),
                new JdbcColumnHandle("test", "text", VARCHAR)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, BIGINT, VARCHAR));

        recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of());
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of());
    }

    @Test
    public void testCursorSimple()
            throws Exception
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of(
                columnHandles.get("text"),
                columnHandles.get("text_short"),
                columnHandles.get("value")));

        try (RecordCursor cursor = recordSet.cursor()) {
            assertEquals(cursor.getType(0), createVarcharType(255));
            assertEquals(cursor.getType(1), createVarcharType(32));
            assertEquals(cursor.getType(2), BIGINT);

            Map<String, Long> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(2));
                assertEquals(cursor.getSlice(0), cursor.getSlice(1));
                assertFalse(cursor.isNull(0));
                assertFalse(cursor.isNull(1));
                assertFalse(cursor.isNull(2));
            }

            assertEquals(data, ImmutableMap.<String, Long>builder()
                    .put("one", 1L)
                    .put("two", 2L)
                    .put("three", 3L)
                    .put("ten", 10L)
                    .put("eleven", 11L)
                    .put("twelve", 12L)
                    .build());
        }
    }

    @Test
    public void testCursorMixedOrder()
            throws Exception
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")));

        try (RecordCursor cursor = recordSet.cursor()) {
            assertEquals(cursor.getType(0), BIGINT);
            assertEquals(cursor.getType(1), BIGINT);
            assertEquals(cursor.getType(2), createVarcharType(255));

            Map<String, Long> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                assertEquals(cursor.getLong(0), cursor.getLong(1));
                data.put(cursor.getSlice(2).toStringUtf8(), cursor.getLong(0));
            }

            assertEquals(data, ImmutableMap.<String, Long>builder()
                    .put("one", 1L)
                    .put("two", 2L)
                    .put("three", 3L)
                    .put("ten", 10L)
                    .put("eleven", 11L)
                    .put("twelve", 12L)
                    .build());
        }
    }

    @Test
    public void testCursorArray()
            throws Exception
    {
        Map<String, JdbcColumnHandle> columnHandles = database.getColumnHandles(
                "exa_ple", "table_with_array_col");
        JdbcSplit split = database.getSplit("exa_ple", "table_with_array_col");

        RecordSet recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of(
                columnHandles.get("col1"),
                columnHandles.get("col2"),
                columnHandles.get("col3"),
                columnHandles.get("col4")));

        try (RecordCursor cursor = recordSet.cursor()) {
            assertEquals(cursor.getType(0), new ArrayType(IntegerType.INTEGER));
            assertEquals(cursor.getType(1), new ArrayType(createVarcharType(255)));
            assertEquals(cursor.getType(2), new ArrayType(DoubleType.DOUBLE));
            assertEquals(cursor.getType(3), new ArrayType(BooleanType.BOOLEAN));

            ImmutableSet.Builder<List<Long>> col1 = ImmutableSet.builder();
            ImmutableSet.Builder<List<String>> col2 = ImmutableSet.builder();
            ImmutableSet.Builder<List<Double>> col3 = ImmutableSet.builder();
            ImmutableSet.Builder<List<Boolean>> col4 = ImmutableSet.builder();
            while (cursor.advanceNextPosition()) {
                Block col1Block = (Block) cursor.getObject(0);
                Block col2Block = (Block) cursor.getObject(1);
                Block col3Block = (Block) cursor.getObject(2);
                Block col4Block = (Block) cursor.getObject(3);

                ImmutableList.Builder<Long> longList = ImmutableList.builder();
                for (int i = 0; i < col1Block.getPositionCount(); i++) {
                    longList.add(IntegerType.INTEGER.getLong(col1Block, i));
                }
                col1.add(longList.build());

                ImmutableList.Builder<String> stringList = ImmutableList.builder();
                for (int i = 0; i < col2Block.getPositionCount(); i++) {
                    stringList.add(createVarcharType(255).getSlice(col2Block, i).toStringUtf8());
                }
                col2.add(stringList.build());

                ImmutableList.Builder<Double> doubleList = ImmutableList.builder();
                for (int i = 0; i < col3Block.getPositionCount(); i++) {
                    doubleList.add(DoubleType.DOUBLE.getDouble(col3Block, i));
                }
                col3.add(doubleList.build());

                ImmutableList.Builder<Boolean> booleanList = ImmutableList.builder();
                for (int i = 0; i < col4Block.getPositionCount(); i++) {
                    booleanList.add(BooleanType.BOOLEAN.getBoolean(col4Block, i));
                }
                col4.add(booleanList.build());
            }

            assertEquals(col1.build(), ImmutableSet.of(
                    ImmutableList.of(1L, 2L, 3L),
                    ImmutableList.of(2L, 3L, 4L)));
            assertEquals(col2.build(), ImmutableSet.of(
                    ImmutableList.of("one", "two", "three"),
                    ImmutableList.of("two", "three", "four")));
            assertEquals(col3.build(), ImmutableSet.of(
                    ImmutableList.of(1.0D, 2.0D, 3.0D),
                    ImmutableList.of(2.0D, 3.0D, 4.0D)));
            assertEquals(col4.build(), ImmutableSet.of(
                    ImmutableList.of(false, true, false),
                    ImmutableList.of(false, true, false)));
        }
    }

    @Test
    public void testIdempotentClose()
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, split, ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")));

        RecordCursor cursor = recordSet.cursor();
        cursor.close();
        cursor.close();
    }
}
