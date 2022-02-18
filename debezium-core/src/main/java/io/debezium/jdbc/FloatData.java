package io.debezium.jdbc;

import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * <p>
 *     A date representing a calendar day with no time of day or timezone. The corresponding Java type is a java.util.Date
 *     with hours, minutes, seconds, milliseconds set to 0. The underlying representation is an integer representing the
 *     number of standardized days (based on a number of milliseconds with 24 hours/day, 60 minutes/hour, 60 seconds/minute,
 *     1000 milliseconds/second with n) since Unix epoch.
 * </p>
 */
public class FloatData {
    public static final String LOGICAL_NAME = "com.info.kafka.connect.data.Float";

    public static SchemaBuilder builder(JdbcValueConverters.DecimalMode mode) {
        switch (mode) {
            case STRING:
                return SchemaBuilder.string().name(LOGICAL_NAME).version(1);
            default:
                return SchemaBuilder.float64().name(LOGICAL_NAME).version(1);
        }
    }
}
