/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase;

import com.kingbase8.util.KSQLException;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for Postgres.
 *
 * @author Gunnar Morling
 */
public class KingbaseESErrorHandler extends ErrorHandler {

    public KingbaseESErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(KingbaseESConnector.class, logicalName, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable instanceof KSQLException
                && (throwable.getMessage().contains("Database connection failed when writing to copy")
                        || throwable.getMessage().contains("Database connection failed when reading from copy"))
                || throwable.getMessage().contains("FATAL: terminating connection due to administrator command")) {
            return true;
        }

        return false;
    }
}
