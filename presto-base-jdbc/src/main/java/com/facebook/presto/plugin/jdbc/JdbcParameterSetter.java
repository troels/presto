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

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcParameterSetter
{
    Connection getConnection() throws SQLException;

    void setNull(int parameterIndex, Type type) throws SQLException;

    void setObject(int parameterIndex, Object object, Type type) throws SQLException;

    void setBoolean(int parameterIndex, boolean value) throws SQLException;

    void setByte(int parameterIndex, byte value) throws SQLException;

    void setShort(int parameterIndex, short value) throws SQLException;

    void setInt(int parameterIndex, int value) throws SQLException;

    void setLong(int parameterIndex, long value) throws SQLException;

    void setFloat(int parameterIndex, float value) throws SQLException;

    void setDouble(int parameterIndex, double value) throws SQLException;

    void setBigDecimal(int parameterIndex, BigDecimal value) throws SQLException;

    void setString(int parameterIndex, String value) throws SQLException;

    void setBytes(int parameterIndex, byte[] value) throws SQLException;

    void setDate(int parameterIndex, java.sql.Date value) throws SQLException;

    void setTime(int parameterIndex, java.sql.Time value) throws SQLException;

    void setTimestamp(int parameterIndex, java.sql.Timestamp value) throws SQLException;

    void setArray(int parameterIndex, Array value) throws SQLException;
}
