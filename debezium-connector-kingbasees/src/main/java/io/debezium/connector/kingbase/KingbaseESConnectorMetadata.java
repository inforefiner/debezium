/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase;

import org.apache.kafka.connect.connector.Connector;

import io.debezium.config.Field;
import io.debezium.metadata.AbstractConnectorMetadata;
import io.debezium.metadata.ConnectorDescriptor;

public class KingbaseESConnectorMetadata extends AbstractConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("kingbase", "Debezium KingbaseSQL Connector", getConnector().version());
    }

    @Override
    public Connector getConnector() {
        return new KingbaseESConnector();
    }

    @Override
    public Field.Set getAllConnectorFields() {
        return KingbaseESConnectorConfig.ALL_FIELDS;
    }

}
