/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase.connection;

import io.debezium.connector.kingbase.KingbaseESConnectorConfig;
import io.debezium.connector.kingbase.KingbaseESSchema;

/**
 * Contextual data required by {@link MessageDecoder}s.
 *
 * @author Chris Cranford
 */
public class MessageDecoderContext {

    private final KingbaseESConnectorConfig config;
    private final KingbaseESSchema schema;

    public MessageDecoderContext(KingbaseESConnectorConfig config, KingbaseESSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    public KingbaseESConnectorConfig getConfig() {
        return config;
    }

    public KingbaseESSchema getSchema() {
        return schema;
    }
}
