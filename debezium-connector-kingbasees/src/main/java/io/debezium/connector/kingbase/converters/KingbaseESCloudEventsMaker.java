/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase.converters;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * CloudEvents maker for records producer by the PostgreSQL connector.
 *
 * @author Chris Cranford
 */
public class KingbaseESCloudEventsMaker extends CloudEventsMaker {

    public KingbaseESCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        super(parser, contentType, dataSchemaUriBase);
    }

    @Override
    public String ceId() {
        return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";lsn:" + recordParser.getMetadata(KingbaseESRecordParser.LSN_KEY).toString()
                + ";txId:" + recordParser.getMetadata(KingbaseESRecordParser.TXID_KEY).toString();
    }
}
