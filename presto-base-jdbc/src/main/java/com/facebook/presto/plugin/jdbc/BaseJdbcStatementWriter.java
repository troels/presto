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

import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import org.joda.time.DateTimeZone;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.joda.time.DateTimeZone.UTC;

public class BaseJdbcStatementWriter
        implements JdbcStatementWriter
{
    private final JdbcClient jdbcClient;

    public BaseJdbcStatementWriter(JdbcClient client)
    {
        this.jdbcClient = requireNonNull(client, "JdbcClient must not be null");
    }

    @Override
    public void setNull(JdbcParameterSetter setter, int index, Type type) throws SQLException
    {
        setter.setNull(index, type);
    }

    @Override
    public void setBoolean(JdbcParameterSetter setter, int index, Type type, boolean value) throws SQLException
    {
        setter.setBoolean(index, value);
    }

    @Override
    public void setLong(JdbcParameterSetter setter, int index, Type type, long value) throws SQLException
    {
        if (BIGINT.equals(type)) {
            setter.setLong(index, value);
        }
        else if (INTEGER.equals(type)) {
            setter.setInt(index, (int) value);
        }
        else if (SMALLINT.equals(type)) {
            setter.setShort(index, (short) value);
        }
        else if (TINYINT.equals(type)) {
            setter.setByte(index, (byte) value);
        }
        else if (REAL.equals(type)) {
            setter.setFloat(index, intBitsToFloat((int) value));
        }
        else if (DATE.equals(type)) {
            long millis = DAYS.toMillis(value);
            setter.setDate(index, new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis)));
        }
        else if (TIME.equals(type)) {
            setter.setTime(index, new Time(value));
        }
        else if (TIME_WITH_TIME_ZONE.equals(type)) {
            setter.setTime(index, new Time(unpackMillisUtc(value)));
        }
        else if (TIMESTAMP.equals(type)) {
            setter.setTimestamp(index, new Timestamp(value));
        }
        else if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            setter.setTimestamp(index, new Timestamp(unpackMillisUtc(value)));
        }
        else {
            throw new UnsupportedOperationException("Can't handle type: " + type);
        }
    }

    @Override
    public void setDouble(JdbcParameterSetter setter, int index, Type type, double value) throws SQLException
    {
        setter.setDouble(index, value);
    }

    @Override
    public void setString(JdbcParameterSetter setter, int index, Type type, byte[] value) throws SQLException
    {
        if (type instanceof VarcharType || type instanceof CharType) {
            setter.setString(index, new String(value, StandardCharsets.UTF_8));
        }
        else if (VARBINARY.equals(type)) {
            setter.setBytes(index, value);
        }
        else {
            throw new UnsupportedOperationException("Can't handle type: " + type);
        }
    }

    @Override
    public void setObject(JdbcParameterSetter setter, int index, Type type, Object value) throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
