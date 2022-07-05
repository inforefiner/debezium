/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase.connection;

import java.sql.SQLException;
import java.time.*;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kingbase8.geometric.*;
import com.kingbase8.jdbc.KbArray;
import com.kingbase8.util.KBInterval;
import com.kingbase8.util.KBmoney;

import io.debezium.connector.kingbase.KingbaseESStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.kingbase.KingbaseESType;
import io.debezium.connector.kingbase.KingbaseESValueConverter;
import io.debezium.connector.kingbase.TypeRegistry;
import io.debezium.connector.kingbase.connection.wal2json.DateTimeFormat;

/**
 * @author Chris Cranford
 */
public abstract class AbstractColumnValue<T> implements ReplicationMessage.ColumnValue<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractColumnValue.class);

    @Override
    public LocalDate asLocalDate() {
        return DateTimeFormat.get().date(asString());
    }

    @Override
    public Object asTime() {
        return asString();
    }

    @Override
    public Object asLocalTime() {
        return DateTimeFormat.get().time(asString());
    }

    @Override
    public OffsetTime asOffsetTimeUtc() {
        return DateTimeFormat.get().timeWithTimeZone(asString());
    }

    @Override
    public OffsetDateTime asOffsetDateTimeAtUtc() {
        if ("infinity".equals(asString())) {
            return KingbaseESValueConverter.POSITIVE_INFINITY_OFFSET_DATE_TIME;
        }
        else if ("-infinity".equals(asString())) {
            return KingbaseESValueConverter.NEGATIVE_INFINITY_OFFSET_DATE_TIME;
        }
        return DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime(asString()).withOffsetSameInstant(ZoneOffset.UTC);
    }

    @Override
    public Instant asInstant() {
        if ("infinity".equals(asString())) {
            return KingbaseESValueConverter.POSITIVE_INFINITY_INSTANT;
        }
        else if ("-infinity".equals(asString())) {
            return KingbaseESValueConverter.NEGATIVE_INFINITY_INSTANT;
        }
        return DateTimeFormat.get().timestampToInstant(asString());
    }

    @Override
    public KBbox asBox() {
        try {
            return new KBbox(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public KBcircle asCircle() {
        try {
            return new KBcircle(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse circle {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public Object asInterval() {
        try {
            return new KBInterval(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public KBline asLine() {
        try {
            return new KBline(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public KBlseg asLseg() {
        try {
            return new KBlseg(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public KBmoney asMoney() {
        try {
            final String value = asString();
            if (value != null && value.startsWith("-")) {
                final String negativeMoney = "(" + value.substring(1) + ")";
                return new KBmoney(negativeMoney);
            }
            return new KBmoney(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse money {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public KBpath asPath() {
        try {
            return new KBpath(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public KBpoint asPoint() {
        try {
            return new KBpoint(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public KBpolygon asPolygon() {
        try {
            return new KBpolygon(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public boolean isArray(KingbaseESType type) {
        return type.isArrayType();
    }

    @Override
    public Object asArray(String columnName, KingbaseESType type, String fullType, PgConnectionSupplier connection) {
        try {
            final String dataString = asString();
            return new KbArray(connection.get(), type.getOid(), dataString);
        }
        catch (SQLException e) {
            LOGGER.warn("Unexpected exception trying to process PgArray ({}) column '{}', {}", fullType, columnName, e);
        }
        return null;
    }

    @Override
    public Object asDefault(TypeRegistry typeRegistry, int columnType, String columnName, String fullType, boolean includeUnknownDatatypes,
                            PgConnectionSupplier connection) {
        if (includeUnknownDatatypes) {
            // this includes things like PostGIS geoemetries or other custom types
            // leave up to the downstream message recipient to deal with
            LOGGER.debug("processing column '{}' with unknown data type '{}' as byte array", columnName, fullType);
            return asString();
        }
        LOGGER.debug("Unknown column type {} for column {} – ignoring", fullType, columnName);
        return null;
    }
}
