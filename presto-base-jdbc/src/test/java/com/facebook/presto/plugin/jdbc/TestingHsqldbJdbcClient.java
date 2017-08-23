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

import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.log.Logger;
import org.hsqldb.jdbc.JDBCDriver;

import java.sql.Types;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestingHsqldbJdbcClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(TestingHsqldbJdbcClient.class);

    public TestingHsqldbJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config, String identifierQuote)
    {
        super(connectorId, config, identifierQuote, new JDBCDriver());
    }

    private static final Pattern VARCHAR_ARRAY_PATTERN = Pattern.compile("^VARCHAR\\((\\d+)\\) ARRAY$");

    @Override
    protected Type toPrestoType(int jdbcType, int columnSize, String typeName)
    {
        if (jdbcType == Types.ARRAY) {
            if ("INTEGER ARRAY".equals(typeName)) {
                return new ArrayType(IntegerType.INTEGER);
            }
            else if ("DOUBLE ARRAY".equals(typeName)) {
                return new ArrayType(DoubleType.DOUBLE);
            }
            else if ("BOOLEAN ARRAY".equals(typeName)) {
                return new ArrayType(BooleanType.BOOLEAN);
            }
            Matcher m = VARCHAR_ARRAY_PATTERN.matcher(typeName);
            if (m.matches()) {
                return new ArrayType(VarcharType.createVarcharType(Integer.parseInt(m.group(1))));
            }
        }
        return super.toPrestoType(jdbcType, columnSize, typeName);
    }
}
