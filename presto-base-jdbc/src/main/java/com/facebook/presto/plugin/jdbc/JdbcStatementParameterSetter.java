package com.facebook.presto.plugin.jdbc;
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
import com.facebook.presto.spi.type.Type;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import static java.util.Objects.requireNonNull;

public class JdbcStatementParameterSetter
        implements JdbcParameterSetter
{
    private final PreparedStatement statement;

    public JdbcStatementParameterSetter(PreparedStatement statement)
    {
        this.statement = requireNonNull(statement, "Statement must not be null");
    }

    @Override
    public Connection getConnection() throws SQLException
    {
        return statement.getConnection();
    }

    @Override
    public void setNull(int parameterIndex, Type type) throws SQLException
    {
        statement.setObject(parameterIndex, null);
    }

    @Override
    public void setObject(int parameterIndex, Object object, Type type) throws SQLException
    {
        statement.setObject(parameterIndex, object);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean value) throws SQLException
    {
        statement.setBoolean(parameterIndex, value);
    }

    @Override
    public void setByte(int parameterIndex, byte value) throws SQLException
    {
        statement.setByte(parameterIndex, value);
    }

    @Override
    public void setShort(int parameterIndex, short value) throws SQLException
    {
        statement.setShort(parameterIndex, value);
    }

    @Override
    public void setInt(int parameterIndex, int value) throws SQLException
    {
        statement.setInt(parameterIndex, value);
    }

    @Override
    public void setLong(int parameterIndex, long value) throws SQLException
    {
        statement.setLong(parameterIndex, value);
    }

    @Override
    public void setFloat(int parameterIndex, float value) throws SQLException
    {
        statement.setFloat(parameterIndex, value);
    }

    @Override
    public void setDouble(int parameterIndex, double value) throws SQLException
    {
        statement.setDouble(parameterIndex, value);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal value) throws SQLException
    {
        statement.setBigDecimal(parameterIndex, value);
    }

    @Override
    public void setString(int parameterIndex, String value) throws SQLException
    {
        statement.setString(parameterIndex, value);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] value) throws SQLException
    {
        statement.setBytes(parameterIndex, value);
    }

    @Override
    public void setDate(int parameterIndex, Date value) throws SQLException
    {
        statement.setDate(parameterIndex, value);
    }

    @Override
    public void setTime(int parameterIndex, Time value) throws SQLException
    {
        statement.setTime(parameterIndex, value);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp value) throws SQLException
    {
        statement.setTimestamp(parameterIndex, value);
    }

    @Override
    public void setArray(int parameterIndex, Array value) throws SQLException
    {
        statement.setArray(parameterIndex, value);
    }
}
