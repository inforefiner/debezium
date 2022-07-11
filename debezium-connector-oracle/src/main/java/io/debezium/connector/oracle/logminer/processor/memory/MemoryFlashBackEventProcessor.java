/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.memory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.*;
import io.debezium.connector.oracle.logminer.LogMinerChangeRecordEmitter;
import io.debezium.connector.oracle.logminer.SqlUtils;
import io.debezium.connector.oracle.logminer.events.*;
import io.debezium.connector.oracle.logminer.parser.DmlParserException;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;
import io.debezium.connector.oracle.logminer.processor.AbstractLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.TransactionCache;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * A {@link LogMinerEventProcessor} that uses the JVM heap to store events as they're being
 * processed and emitted from Oracle LogMiner.
 *
 * @author Chris Cranford
 */
public class MemoryFlashBackEventProcessor extends AbstractLogMinerEventProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryFlashBackEventProcessor.class);

    private final ChangeEventSourceContext context;
    private final OracleConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final OracleStreamingChangeEventSourceMetrics metrics;
    private final MemoryTransactionCache transactionCache;
    private final Map<String, Scn> recentlyCommittedTransactionsCache = new HashMap<>();
    private final Set<Scn> schemaChangesCache = new HashSet<>();
    private final Set<String> abandonedTransactionsCache = new HashSet<>();
    private final Set<String> rollbackTransactionsCache = new HashSet<>();

    private Scn currentOffsetScn = Scn.NULL;
    private Scn currentOffsetCommitScn = Scn.NULL;
    private Scn lastCommittedScn = Scn.NULL;
    private Scn maxCommittedScn = Scn.NULL;

    public MemoryFlashBackEventProcessor(ChangeEventSourceContext context,
                                         OracleConnectorConfig connectorConfig,
                                         OracleConnection jdbcConnection,
                                         EventDispatcher<TableId> dispatcher,
                                         OraclePartition partition,
                                         OracleOffsetContext offsetContext,
                                         OracleDatabaseSchema schema,
                                         OracleStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, schema, partition, offsetContext, dispatcher, metrics);
        this.context = context;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.metrics = metrics;
        this.transactionCache = new MemoryTransactionCache();
    }

    @Override
    protected TransactionCache<?> getTransactionCache() {
        return transactionCache;
    }

    @Override
    public void close() throws Exception {
        // close any resources used here
    }

    /**
     * Processes the LogMiner results.
     *
     * @param resultSet the result set from a LogMiner query
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the dispatcher was interrupted sending an event
     */
    @Override
    protected void processResults(ResultSet resultSet) throws SQLException, InterruptedException {
        while (context.isRunning() && hasNextWithMetricsUpdate(resultSet)) {
            counters.rows++;
            processRow(LogMinerEventRow.fromResultSetByFlashBack(resultSet, getConfig().getCatalogName(), isTrxIdRawValue()));
        }
    }

    /**
     * Processes a single LogMinerEventRow.
     *
     * @param row the event row, must not be {@code null}
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the dispatcher was interrupted sending an event
     */
    @Override
    protected void processRow(LogMinerEventRow row) throws SQLException, InterruptedException {
        if (!row.getEventType().equals(EventType.MISSING_SCN)) {
            this.lastProcessedScn = row.getScn();
        }

        if (!getConfig().getTableFilters().dataCollectionFilter().isIncluded(row.getTableId())) {
            return;
        }

        final TableId tableId = row.getTableId();
        Table table = getSchema().tableFor(tableId);
        if (table == null) {
            table = dispatchSchemaChangeEventAndGetTableForNewCapturedTable(tableId, offsetContext, dispatcher);
        }
        switch (row.getEventType()) {
            case UNSUPPORTED:
                LOGGER.warn("Unsupported event type: {}", row.getEventType());
                break;
            case INSERT:
            case UPDATE:
            case DELETE:

                if (transactionCache.cache.containsKey(row.getTransactionId()) && row.getCommitScn() != null && !row.getCommitScn().isNull()) {
                    handleCommit(row);
                }

                LogMinerDmlEntry dmlEntry = parse(row.getUndoSql(), row.getTableName(), table, row.getRowId());
                dmlEntry.setObjectName(row.getTableName());
                dmlEntry.setObjectOwner(row.getTablespaceName());
                addToTransaction(row.getTransactionId(), row, () -> new DmlEvent(row, dmlEntry));
                if (row.getCommitScn() != null && !row.getCommitScn().isNull()) {
                    handleCommit(row);
                }
                break;
        }
    }

    @Override
    public Scn process(Scn startScn, Scn endScn) throws SQLException, InterruptedException {
        counters.reset();

        try (PreparedStatement statement = createQueryStatement()) {
            LOGGER.debug("Fetching results for SCN [{}, {}]", startScn, endScn);
            statement.setFetchSize(getConfig().getMaxQueueSize());
            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
            statement.setString(1, startScn.toString());
            statement.setString(2, startScn.toString());
            statement.setString(3, endScn.toString());

            Instant queryStart = Instant.now();
            try (ResultSet resultSet = statement.executeQuery()) {
                metrics.setLastDurationOfBatchCapturing(Duration.between(queryStart, Instant.now()));

                Instant startProcessTime = Instant.now();
                processResults(resultSet);

                Duration totalTime = Duration.between(startProcessTime, Instant.now());
                metrics.setLastCapturedDmlCount(counters.dmlCount);
                metrics.setLastDurationOfBatchCapturing(totalTime);

                if (counters.dmlCount > 0 || counters.commitCount > 0 || counters.rollbackCount > 0) {
                    warnPotentiallyStuckScn(currentOffsetScn, currentOffsetCommitScn);

                    currentOffsetScn = offsetContext.getScn();
                    if (offsetContext.getCommitScn() != null) {
                        currentOffsetCommitScn = offsetContext.getCommitScn();
                    }
                }

                LOGGER.debug("{}.", counters);
                LOGGER.debug("Processed in {} ms. Log: {}. Offset SCN: {}, Offset Commit SCN: {}, Active Transactions: {}, Sleep: {}",
                        totalTime.toMillis(), metrics.getLagFromSourceInMilliseconds(), offsetContext.getScn(),
                        offsetContext.getCommitScn(), metrics.getNumberOfActiveTransactions(),
                        metrics.getMillisecondToSleepBetweenMiningQuery());

                metrics.addProcessedRows(counters.rows);
                return calculateNewStartScn(endScn);
            }
        }
    }

    @Override
    public void abandonTransactions(Duration retention) {
        if (!Duration.ZERO.equals(retention)) {
            final Scn offsetScn = offsetContext.getScn();
            Optional<Scn> lastScnToAbandonTransactions = getLastScnToAbandon(jdbcConnection, offsetScn, retention);
            lastScnToAbandonTransactions.ifPresent(thresholdScn -> {
                LOGGER.warn("All transactions with SCN <= {} will be abandoned.", thresholdScn);
                Scn smallestScn = transactionCache.getMinimumScn();
                if (!smallestScn.isNull()) {
                    if (thresholdScn.compareTo(smallestScn) < 0) {
                        thresholdScn = smallestScn;
                    }

                    Iterator<Map.Entry<String, Transaction>> iterator = transactionCache.iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, Transaction> entry = iterator.next();
                        if (entry.getValue().getStartScn().compareTo(thresholdScn) <= 0) {
                            LOGGER.warn("Transaction {} is being abandoned.", entry.getKey());
                            abandonedTransactionsCache.add(entry.getKey());
                            iterator.remove();

                            metrics.addAbandonedTransactionId(entry.getKey());
                            metrics.setActiveTransactions(transactionCache.size());
                        }
                    }

                    // Update the oldest scn metric are transaction abandonment
                    smallestScn = transactionCache.getMinimumScn();
                    metrics.setOldestScn(smallestScn.isNull() ? Scn.valueOf(-1) : smallestScn);
                }

                offsetContext.setScn(thresholdScn);
            });
        }
    }

    @Override
    protected boolean isRecentlyCommitted(String transactionId) {
        return recentlyCommittedTransactionsCache.containsKey(transactionId);
    }

    @Override
    protected boolean isTransactionIdAllowed(String transactionId) {
        if (abandonedTransactionsCache.contains(transactionId)) {
            LOGGER.warn("Event for abandoned transaction {}, skipped.", transactionId);
            return false;
        }
        if (rollbackTransactionsCache.contains(transactionId)) {
            LOGGER.warn("Event for rolled back transaction {}, skipped.", transactionId);
            return false;
        }
        if (recentlyCommittedTransactionsCache.containsKey(transactionId)) {
            LOGGER.trace("Event for already committed transaction {}, skipped.", transactionId);
            return false;
        }
        return true;
    }

    @Override
    protected boolean hasSchemaChangeBeenSeen(LogMinerEventRow row) {
        return schemaChangesCache.contains(row.getScn());
    }

    @Override
    protected void handleCommit(LogMinerEventRow row) throws InterruptedException {
        final String transactionId = row.getTransactionId();
        if (recentlyCommittedTransactionsCache.containsKey(transactionId)) {
            return;
        }

        final Scn smallestScn = transactionCache.getMinimumScn();

        final Transaction transaction = transactionCache.remove(transactionId);
        if (transaction == null) {
            LOGGER.trace("Transaction {} not found.", transactionId);
            return;
        }
        metrics.setOldestScn(smallestScn.isNull() ? Scn.valueOf(-1) : smallestScn);
        abandonedTransactionsCache.remove(transactionId);

        final Scn commitScn = row.getScn();
        final Scn offsetCommitScn = offsetContext.getCommitScn();
        if ((offsetCommitScn != null && offsetCommitScn.compareTo(commitScn) > 0) || lastCommittedScn.compareTo(commitScn) > 0) {
            LOGGER.debug("Transaction {} has already been processed. Commit SCN in offset is {} while commit SCN " +
                    "of transaction is {} and last seen committed SCN is {}.",
                    transactionId, offsetCommitScn, commitScn, lastCommittedScn);
            metrics.setActiveTransactions(transactionCache.size());
            return;
        }

        counters.commitCount++;
        Instant start = Instant.now();
        getReconciliation().reconcile(transaction);

        int numEvents = transaction.getEvents().size();

        LOGGER.trace("Commit: (smallest SCN {}) {}", smallestScn, row);
        LOGGER.trace("Transaction {} has {} events", transactionId, numEvents);

        for (LogMinerEvent event : transaction.getEvents()) {
            if (!context.isRunning()) {
                return;
            }

            // Update SCN in offset context only if processed SCN less than SCN of other transactions
            if (smallestScn.isNull() || commitScn.compareTo(smallestScn) < 0) {
                offsetContext.setScn(event.getScn());
                metrics.setOldestScn(event.getScn());
            }

            offsetContext.setTransactionId(transactionId);
            offsetContext.setSourceTime(event.getChangeTime());
            offsetContext.setTableId(event.getTableId());
            if (--numEvents == 0) {
                // reached the last event update the commit scn in the offsets
                offsetContext.setCommitScn(commitScn);
            }

            // after reconciliation all events should be DML
            final DmlEvent dmlEvent = (DmlEvent) event;
            dispatcher.dispatchDataChangeEvent(event.getTableId(),
                    new LogMinerChangeRecordEmitter(
                            partition,
                            offsetContext,
                            dmlEvent.getEventType(),
                            dmlEvent.getDmlEntry().getOldValues(),
                            dmlEvent.getDmlEntry().getNewValues(),
                            getSchema().tableFor(event.getTableId()),
                            Clock.system()));
        }

        lastCommittedScn = Scn.valueOf(commitScn.longValue());
        if (!transaction.getEvents().isEmpty()) {
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext);
        }
        else {
            dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
        }

        metrics.calculateLagMetrics(row.getChangeTime());
        if (lastCommittedScn.compareTo(maxCommittedScn) > 0) {
            maxCommittedScn = lastCommittedScn;
        }

        if (getConfig().isLobEnabled()) {
            // cache recently committed transactions by transaction id
            recentlyCommittedTransactionsCache.put(transactionId, commitScn);
        }

        metrics.incrementCommittedTransactions();
        metrics.setActiveTransactions(transactionCache.size());
        metrics.incrementCommittedDmlCount(transaction.getEvents().size());
        metrics.setCommittedScn(commitScn);
        metrics.setOffsetScn(offsetContext.getScn());
        metrics.setLastCommitDuration(Duration.between(start, Instant.now()));
    }

    @Override
    protected void handleRollback(LogMinerEventRow row) {
        final Transaction transaction = transactionCache.get(row.getTransactionId());
        if (transaction != null) {
            transactionCache.remove(row.getTransactionId());
            abandonedTransactionsCache.remove(row.getTransactionId());
            rollbackTransactionsCache.add(row.getTransactionId());

            metrics.setActiveTransactions(transactionCache.size());
            metrics.incrementRolledBackTransactions();
            metrics.addRolledBackTransactionId(row.getTransactionId());

            counters.rollbackCount++;
        }
    }

    @Override
    protected void handleSchemaChange(LogMinerEventRow row) throws InterruptedException {
        super.handleSchemaChange(row);
        if (row.getTableName() != null) {
            schemaChangesCache.add(row.getScn());
        }
    }

    private PreparedStatement createQueryStatement() throws SQLException {
        // final String query = LogMinerQueryBuilder.build(getConfig());
        final String query = "SELECT xid, start_scn, start_timestamp, commit_scn, commit_timestamp, logon_user, undo_change#, operation, table_name, table_owner, row_id, undo_sql\n"
                +
                ",ORA_HASH(COMMIT_SCN||OPERATION||ROW_ID||RTRIM(SUBSTR(UNDO_SQL,1,256))) as HASH FROM flashback_transaction_query where START_SCN > ? and COMMIT_SCN > ? and COMMIT_SCN <= ? "
                +
                "AND operation IN ('UPDATE','DELETE','INSERT') ORDER BY COMMIT_SCN";
        return jdbcConnection.connection().prepareStatement(query,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    private Scn calculateNewStartScn(Scn endScn) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            if (transactionCache.isEmpty() && !maxCommittedScn.isNull()) {
                offsetContext.setScn(maxCommittedScn);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            else {
                final Scn minStartScn = transactionCache.getMinimumScn();
                if (!minStartScn.isNull()) {
                    recentlyCommittedTransactionsCache.entrySet().removeIf(entry -> entry.getValue().compareTo(minStartScn) < 0);
                    schemaChangesCache.removeIf(scn -> scn.compareTo(minStartScn) < 0);
                    offsetContext.setScn(minStartScn.subtract(Scn.valueOf(1)));
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                }
            }
            return offsetContext.getScn();
        }
        else {
            if (!getLastProcessedScn().isNull() && getLastProcessedScn().compareTo(endScn) < 0) {
                // If the last processed SCN is before the endScn we need to use the last processed SCN as the
                // next starting point as the LGWR buffer didn't flush all entries from memory to disk yet.
                endScn = getLastProcessedScn();
            }

            if (transactionCache.isEmpty()) {
                offsetContext.setScn(endScn);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            else {
                final Scn minStartScn = transactionCache.getMinimumScn();
                if (!minStartScn.isNull()) {
                    offsetContext.setScn(minStartScn.subtract(Scn.valueOf(1)));
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                }
            }
            return endScn;
        }
    }

    /**
     * Calculates the SCN as a watermark to abandon for long running transactions.
     * The criteria is do not let the offset SCN expire from archives older the specified retention hours.
     *
     * @param connection database connection, should not be {@code null}
     * @param offsetScn offset system change number, should not be {@code null}
     * @param retention duration to tolerate long running transactions before being abandoned, must not be {@code null}
     * @return an optional system change number as the watermark for transaction buffer abandonment
     */
    protected Optional<Scn> getLastScnToAbandon(OracleConnection connection, Scn offsetScn, Duration retention) {
        try {
            Float diffInDays = connection.singleOptionalValue(SqlUtils.diffInDaysQuery(offsetScn), rs -> rs.getFloat(1));
            if (diffInDays != null && (diffInDays * 24) > retention.toHours()) {
                return Optional.of(offsetScn);
            }
            return Optional.empty();
        }
        catch (SQLException e) {
            LOGGER.error("Cannot calculate days difference for transaction abandonment", e);
            metrics.incrementErrorCount();
            return Optional.of(offsetScn);
        }
    }

    private LogMinerDmlEntry parse(String undoSql, String tableName, Table table, String rowId) {
        if (table == null) {
            throw new DmlParserException("DML parser requires a non-null table");
        }
        if (undoSql != null && undoSql.length() > 0) {
            switch (undoSql.charAt(0)) {
                case 'i':
                    // undo sql start 'i' , is Delete event
                    return parseInsert(undoSql, tableName, table, rowId);
                case 'u':
                    return parseUpdate(undoSql, tableName, table, rowId);
                case 'd':
                    // undo sql start 'd' , is Insert event
                    return parseDelete(undoSql, tableName, table, rowId);
            }
        }
        throw new DmlParserException("Unknown supported SQL '" + undoSql + "'");
    }

    private LogMinerDmlEntry parseInsert(String undoSql, String tableName, Table table, String rowId) {
        LOGGER.info("parse Insert Event, undoSql {} , tableName {}", undoSql, tableName);

        LogMinerDmlEntry parse = dmlParser.parseUndoSql(undoSql, table, null);
        return parse;
    }

    private LogMinerDmlEntry parseUpdate(String undoSql, String tableName, Table table, String rowId) {
        LOGGER.info("parse Update Event, undoSql {} , tableName {}", undoSql, tableName);

        LogMinerDmlEntry parse = dmlParser.parse(undoSql, table, null);

        Object[] newValues = parse.getNewValues();

        final String sql = "SELECT * FROM " + tableName + " WHERE ROWID = ?";
        try (PreparedStatement preparedStatement = jdbcConnection.connection().prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
            // preparedStatement.setString(1, tableName);
            preparedStatement.setString(1, rowId);
            ResultSet resultSet = preparedStatement.executeQuery();
            List<Column> columns = table.columns();
            Object[] oldValues = new Object[columns.size()];
            while (resultSet.next()) {
                for (Column column : columns) {
                    oldValues[column.position() - 1] = resultSet.getObject(column.name());
                }
            }
            for (int i = 0; i < newValues.length; i++) {
                if (newValues[i] == null) {
                    newValues[i] = oldValues[i];
                }
            }
            return LogMinerDmlEntryImpl.forUpdate(oldValues, newValues);
        }
        catch (SQLException e) {
            throw new DmlParserException("Failed to parse insert DML: '" + sql + "', UNDO SQL: " + undoSql, e);
        }

    }

    private LogMinerDmlEntry parseDelete(String undoSql, String tableName, Table table, String rowId) {
        LOGGER.info("parse Delete Event, undoSql {} , tableName {}", undoSql, tableName);

        final String sql = "SELECT * FROM " + tableName + " WHERE ROWID = ?";

        try (PreparedStatement preparedStatement = jdbcConnection.connection().prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
            preparedStatement.setString(1, rowId);
            ResultSet resultSet = preparedStatement.executeQuery();
            List<Column> columns = table.columns();
            Object[] newValues = new Object[columns.size()];
            while (resultSet.next()) {
                for (Column column : columns) {
                    newValues[column.position() - 1] = resultSet.getObject(column.name());
                }
            }
            return LogMinerDmlEntryImpl.forInsert(newValues);
        }
        catch (SQLException e) {
            throw new DmlParserException("Failed to parse insert DML: '" + sql + "', UNDO SQL: " + undoSql, e);
        }
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code INSERT}, {@code UPDATE}, or {@code DELETE} event.
     *
     * @param row the result set row
     * @throws SQLException if a database exception occurs
     * @throws InterruptedException if the dispatch of an event is interrupted
     */
    @Override
    protected void handleDataEvent(LogMinerEventRow row) throws SQLException, InterruptedException {
        if (row.getRedoSql() == null) {
            return;
        }

        LOGGER.trace("DML: {}", row);
        LOGGER.trace("\t{}", row.getRedoSql());

        counters.dmlCount++;
        switch (row.getEventType()) {
            case INSERT:
                counters.insertCount++;
                break;
            case UPDATE:
                counters.updateCount++;
                break;
            case DELETE:
                counters.deleteCount++;
                break;
        }

        final TableId tableId = row.getTableId();
        Table table = getSchema().tableFor(tableId);
        if (table == null) {
            if (!getConfig().getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                return;
            }
            table = dispatchSchemaChangeEventAndGetTableForNewCapturedTable(tableId, offsetContext, dispatcher);
        }

        if (row.isRollbackFlag()) {
            // There is a use case where a constraint violation will result in a DML event being
            // written to the redo log subsequently followed by another DML event that is marked
            // with a rollback flag to indicate that the prior event should be omitted. In this
            // use case, the transaction can still be committed, so we need to manually rollback
            // the previous DML event when this use case occurs.
            final Transaction transaction = getTransactionCache().get(row.getTransactionId());
            if (transaction == null) {
                LOGGER.warn("Cannot undo change '{}' since transaction was not found.", row);
            }
            else {
                transaction.removeEventWithRowId(row.getRowId());
            }
            return;
        }

        final LogMinerDmlEntry dmlEntry = parse(row.getRedoSql(), row.getTableName(), table, row.getTransactionId());
        dmlEntry.setObjectName(row.getTableName());
        dmlEntry.setObjectOwner(row.getTablespaceName());

        addToTransaction(row.getTransactionId(), row, () -> new DmlEvent(row, dmlEntry));

        metrics.incrementRegisteredDmlCount();
    }

}
