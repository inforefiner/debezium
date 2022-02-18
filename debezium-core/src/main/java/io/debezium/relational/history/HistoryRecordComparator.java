/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.Document;

/**
 * Compares HistoryRecord instances to determine which came first.
 *
 * @author Randall Hauch
 * @since 0.2
 */
public class HistoryRecordComparator {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    /**
     * A comparator instance that requires the {@link HistoryRecord#source() records' sources} to be the same and considers only
     * those fields that are in both records' {@link HistoryRecord#position() positions}.
     */
    public static final HistoryRecordComparator INSTANCE = new HistoryRecordComparator();

    /**
     * Create a {@link HistoryRecordComparator} that requires identical sources but will use the supplied function to compare
     * positions.
     *
     * @param positionComparator the non-null function that returns {@code true} if the first position is at or before
     *            the second position or {@code false} otherwise
     * @return the comparator instance; never null
     */
    public static HistoryRecordComparator usingPositions(BiFunction<Document, Document, Boolean> positionComparator) {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document position1, Document position2) {
                return positionComparator.apply(position1, position2);
            }
        };
    }

    /**
     * Determine if the first {@link HistoryRecord} is at the same or earlier point in time than the second {@link HistoryRecord}.
     *
     * @param record1 the first record; never null
     * @param record2 the second record; never null
     * @return {@code true} if the first record is at the same or earlier point in time than the second record, or {@code false}
     *         otherwise
     */
    public boolean isAtOrBefore(HistoryRecord record1, HistoryRecord record2) {
        logger.info("method :{} record1 :{} , record2 :{}", "isAtOrBefore", record1, record2);
        boolean sameSource = isSameSource(record1.source(), record2.source());
        boolean positionAtOrBefore = isPositionAtOrBefore(record1.position(), record2.position());

        logger.info("isSameSource value :{} ", sameSource);
        logger.info("isPositionAtOrBefore value :{} ", positionAtOrBefore);
        return sameSource && positionAtOrBefore;
    }

    protected boolean isPositionAtOrBefore(Document position1, Document position2) {
        return position1.compareToUsingSimilarFields(position2) <= 0;
    }

    protected boolean isSameSource(Document source1, Document source2) {
        logger.info("source1 class:{},source2 class:{}", source1.getClass(), source2.getClass());
        return source1.equalsSource(source2);
    }
}
