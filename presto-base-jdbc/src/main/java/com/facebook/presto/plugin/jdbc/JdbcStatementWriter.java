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

import com.facebook.presto.spi.type.Type;

import java.sql.SQLException;

public interface JdbcStatementWriter
{
    public void setNull(JdbcParameterSetter setter, int index, Type type) throws SQLException;

    public void setBoolean(JdbcParameterSetter setter, int index, Type type, boolean value) throws SQLException;

    public void setLong(JdbcParameterSetter setter, int index, Type type, long value) throws SQLException;

    public void setDouble(JdbcParameterSetter setter, int index, Type type, double value) throws SQLException;

    public void setString(JdbcParameterSetter setter, int index, Type type, byte[] value) throws SQLException;

    public void setObject(JdbcParameterSetter setter, int index, Type type, Object value) throws SQLException;
}
