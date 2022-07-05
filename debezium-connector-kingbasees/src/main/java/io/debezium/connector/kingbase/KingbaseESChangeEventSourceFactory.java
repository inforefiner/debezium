/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbase;

import java.util.Optional;

import io.debezium.connector.kingbase.connection.KingbaseESConnection;
import io.debezium.connector.kingbase.connection.ReplicationConnection;
import io.debezium.connector.kingbase.spi.SlotCreationResult;
import io.debezium.connector.kingbase.spi.SlotState;
import io.debezium.connector.kingbase.spi.Snapshotter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.*;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;

public class KingbaseESChangeEventSourceFactory implements ChangeEventSourceFactory<KingbaseESPartition, KingbaseESOffsetContext> {

    private final KingbaseESConnectorConfig configuration;
    private final KingbaseESConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final KingbaseESSchema schema;
    private final KingbaseESTaskContext taskContext;
    private final Snapshotter snapshotter;
    private final ReplicationConnection replicationConnection;
    private final SlotCreationResult slotCreatedInfo;
    private final SlotState startingSlotInfo;

    public KingbaseESChangeEventSourceFactory(KingbaseESConnectorConfig configuration, Snapshotter snapshotter, KingbaseESConnection jdbcConnection,
                                              ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, KingbaseESSchema schema,
                                              KingbaseESTaskContext taskContext,
                                              ReplicationConnection replicationConnection, SlotCreationResult slotCreatedInfo, SlotState startingSlotInfo) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        this.replicationConnection = replicationConnection;
        this.slotCreatedInfo = slotCreatedInfo;
        this.startingSlotInfo = startingSlotInfo;
    }

    @Override
    public SnapshotChangeEventSource<KingbaseESPartition, KingbaseESOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener snapshotProgressListener) {
        return new KingbaseESSnapshotChangeEventSource(
                configuration,
                snapshotter,
                jdbcConnection,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener,
                slotCreatedInfo,
                startingSlotInfo);
    }

    @Override
    public StreamingChangeEventSource<KingbaseESPartition, KingbaseESOffsetContext> getStreamingChangeEventSource() {
        return new KingbaseESStreamingChangeEventSource(
                configuration,
                snapshotter,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                replicationConnection);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                              KingbaseESOffsetContext offsetContext,
                                                                                                                              SnapshotProgressListener snapshotProgressListener,
                                                                                                                              DataChangeEventListener dataChangeEventListener) {
        final SignalBasedIncrementalSnapshotChangeEventSource<TableId> incrementalSnapshotChangeEventSource = new SignalBasedIncrementalSnapshotChangeEventSource<TableId>(
                configuration,
                jdbcConnection,
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }
}
