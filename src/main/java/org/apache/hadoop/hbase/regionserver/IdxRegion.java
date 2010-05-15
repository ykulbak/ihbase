/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.idx.IdxScan;
import org.apache.hadoop.hbase.client.idx.exp.Expression;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An indexed region - has the capability to index a subset of the columns and speedup scans which specify a
 * filter expression.
 */
public class IdxRegion extends HRegion {
    static final Log LOG = LogFactory.getLog(IdxRegion.class);

    private static final int INDEX_BUILD_TIME_HISTORY_SIZE = 10;

    private static final long FIXED_OVERHEAD = ClassSize.REFERENCE * 2 +
            2 * (ClassSize.REFERENCE + ClassSize.ATOMIC_INTEGER) +
            ClassSize.REFERENCE + ClassSize.ATOMIC_LONG +
            ClassSize.REFERENCE + ClassSize.ARRAY +
            INDEX_BUILD_TIME_HISTORY_SIZE * Bytes.SIZEOF_LONG +
            Bytes.SIZEOF_INT;

    private IdxRegionIndexManager indexManager;
    private IdxExpressionEvaluator expressionEvaluator;

    // todo add a total number of ongoing scans to the HRegion
    private AtomicInteger numberOfOngoingIndexedScans;
    // todo add a a resetable total scan count when HRegion has jmx support
    private AtomicLong totalIndexedScans;
    private AtomicLong totalNonIndexedScans;
    // the index build time history
    private volatile long[] buildTimes;
    private volatile int currentBuildTimesIndex;

    /**
     * A default constructor matching the default region constructor.
     */
    public IdxRegion() {
        super();
    }

    /**
     * See {@link HRegion#HRegion(org.apache.hadoop.fs.Path, HLog, org.apache.hadoop.fs.FileSystem, org.apache.hadoop.hbase.HBaseConfiguration, org.apache.hadoop.hbase.HRegionInfo, FlushRequester)}.
     * <p/>
     * Initializes the index manager and the expression evaluator.
     */
    public IdxRegion(Path basedir, HLog log, FileSystem fs, HBaseConfiguration
            conf, HRegionInfo regionInfo, FlushRequester flushListener) {
        super(basedir, log, fs, conf, regionInfo, flushListener);
        indexManager = new IdxRegionIndexManager(this);
        expressionEvaluator = new IdxExpressionEvaluator();
        // monitoring parameters
        numberOfOngoingIndexedScans = new AtomicInteger(0);
        totalIndexedScans = new AtomicLong(0);
        totalNonIndexedScans = new AtomicLong(0);
        buildTimes = new long[INDEX_BUILD_TIME_HISTORY_SIZE];
        resetIndexBuildTimes();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * calls super and then initialized the index manager.
     */
    @Override
    public void initialize(Path initialFiles, Progressable reporter)
            throws IOException {
        super.initialize(initialFiles, reporter);
        Callable<Void> work = rebuildIndexes();
        try {
            work.call();
        } catch (Exception e) {
            // work can only throw an IOException, this code sucks
            throw (IOException) e;
        }
        JmxHelper.registerMBean(
                IdxRegionMBeanImpl.generateObjectName(getRegionInfo()),
                IdxRegionMBeanImpl.newIdxRegionMBeanImpl(this));
    }

    /**
     * {@inheritDoc}
     * </p>
     * Rebuilds the index.
     */
    @Override
    protected Callable<Void> internalPreFlushcacheCommit() throws IOException {
        return rebuildIndexes();
    }

    private Callable<Void> rebuildIndexes() throws IOException {
        // Need to scan memstore, SO BAM GO
        ReadWriteConsistencyControl.resetThreadReadPoint(rwcc);

        Pair<Long, Callable<Void>> aPair = indexManager.rebuildIndexes();

        long time = aPair.getFirst();
        buildTimes[currentBuildTimesIndex] = time;
        currentBuildTimesIndex = (currentBuildTimesIndex + 1) % buildTimes.length;

        return aPair.getSecond();
    }


    @Override
    public List<StoreFile> close(boolean abort) throws IOException {
        MBeanUtil.unregisterMBean(
                IdxRegionMBeanImpl.generateObjectName(getRegionInfo()));
        return super.close(abort);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Constructs an internal scanner based on the scan expression. Reverts
     * to default region scan in case an expression was not provided.
     */
    @Override
    protected InternalScanner instantiateInternalScanner(Scan scan,
                                                         List<KeyValueScanner> additionalScanners) throws IOException {
        Expression expression = IdxScan.getExpression(scan);
        if (scan == null || expression == null) {
            totalNonIndexedScans.incrementAndGet();
            return super.instantiateInternalScanner(scan, additionalScanners);
        } else {
            return new IdxRegionScanner(scan);
        }
    }

    /**
     * Calculates the average number of key/values in this regions memstores.
     *
     * @return the average number of key values
     */
    int averageNumberOfMemStoreSKeys() {
        int totalKVs = 0;
        for (Store store : stores.values()) {
            totalKVs += store.memstore.numKeyValues();
        }
        return totalKVs / this.stores.size();
    }

    /**
     * A monitoring operation which exposes the number of indexed keys.
     *
     * @return the number of indexed keys.
     */
    public int getNumberOfIndexedKeys() {
        return indexManager.getNumberOfKeys();
    }

    /**
     * The total heap size consumed by all indexes.
     *
     * @return the index heap size.
     */
    public long getIndexesTotalHeapSize() {
        return indexManager.heapSize();
    }

    /**
     * Gets the total number of indexed scan since the last reset.
     *
     * @return the total number of indexed scans.
     */
    public long getTotalIndexedScans() {
        return totalIndexedScans.get();
    }

    /**
     * Resets the total number of indexed scans.
     *
     * @return the number of indexed scans just before the reset
     */
    public long resetTotalIndexedScans() {
        return totalIndexedScans.getAndSet(0);
    }

    /**
     * Gets the total number of non indexed scan since the last reset.
     *
     * @return the total number of indexed scans.
     */
    public long getTotalNonIndexedScans() {
        return totalNonIndexedScans.get();
    }

    /**
     * Resets the total number of non indexed scans.
     *
     * @return the number of indexed scans just before the reset
     */
    public long resetTotalNonIndexedScans() {
        return totalNonIndexedScans.getAndSet(0);
    }

    /**
     * Exposes the number of indexed scans currently ongoing in the system.
     *
     * @return the number of ongoing indexed scans
     */
    public long getNumberOfOngoingIndexedScans() {
        return numberOfOngoingIndexedScans.get();
    }

    /**
     * Gets the index build times buffer.
     *
     * @return a rolling buffer of index build times
     */
    public long[] getIndexBuildTimes() {
        return buildTimes;
    }

    /**
     * Resets the index build times array.
     *
     * @return the previous build times array
     */
    public long[] resetIndexBuildTimes() {
        // note: this may 'corrupt' the array if ran in parallel with a build time
        // array modification and that's ok. we are not changing pointers so
        // no catastrophy should happen - worse case the manager would reset again
        long[] prevTimes = buildTimes.clone();
        Arrays.fill(buildTimes, -1L);
        currentBuildTimesIndex = 0;
        return prevTimes;
    }

    /**
     * The size of the index on the specified column in bytes.
     *
     * @param columnName the column to check.
     * @return the size fo the requesed index
     */
    public long getIndexHeapSize(String columnName) {
        return indexManager.getIndexHeapSize(columnName);
    }

    private IdxRegionScanner lastScanner = null;

    class IdxRegionScanner extends RegionScanner {
        private final KeyProvider keyProvider;
        private KeyValue lastKeyValue;

        IdxRegionScanner(Scan scan) throws IOException {
            super(scan);
            //DebugPrint.println("IdxRegionScanner.<init>");

            Expression expression = IdxScan.getExpression(scan);
            totalIndexedScans.incrementAndGet();

            IdxSearchContext idxSearchContext = indexManager.newSearchContext();

            // use the expression evaluator to determine the final set of ints
            IntSet matchedExpression;
            try {
                matchedExpression = expressionEvaluator.evaluate(
                        idxSearchContext, expression
                );
            } catch (RuntimeException e) {
                throw new DoNotRetryIOException(e.getMessage(), e);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("%s rows matched the index expression",
                        matchedExpression.size()));
            }

            numberOfOngoingIndexedScans.incrementAndGet();
            keyProvider = new KeyProvider(idxSearchContext, matchedExpression, scan);
        }


        @Override
        public boolean next(List<KeyValue> outResults) throws IOException {
            ReadWriteConsistencyControl.resetThreadReadPoint(rwcc);
            if (storeHeap == null) {
                keyProvider.init();
                initHeap();
            }
            //DebugPrint.println("IdxScanner.next");
            // Seek to the next key value
            seekNext();
            boolean result = super.next(outResults);

            // if there are results we need to key track of the key to ensure that the
            // nextInternal method doesn't seek backwards on it's next invocation
            if (!outResults.isEmpty()) {
                lastKeyValue = outResults.get(0);
            }

            lastScanner = this;

            return result;
        }

        /**
         * {@inheritDoc}
         * <p/>
         * Fast forwards the scanner by calling {@link #seekNext()}.
         */
        @Override
        protected void nextRow(byte[] currentRow) throws IOException {
            seekNext();
            super.nextRow(currentRow);
        }

        protected void seekNext() throws IOException {
            KeyValue keyValue;
            do {
                keyValue = keyProvider.next();

                if (keyValue == null) {
                    // out of results keys, nothing more to process
                    super.storeHeap.close();
                    return;
                } else if (lastKeyValue == null) {
                    // first key returned from the key provider
                    break;
                } else {
                    // it's possible that the super nextInternal method progressed past the
                    // ketProvider's next key.  We need to keep calling next on the keyProvider
                    // until the key returned is after the last key returned from the
                    // next(List<KeyValue>) method.

                    // determine which of the two keys is less than the other
                    // when the keyValue is greater than the lastKeyValue then we're good
                    int comparisonResult = comparator.compareRows(keyValue, lastKeyValue);
                    if (comparisonResult > 0) {
                        break;
                    }
                    //DebugPrint.println("seekNext: " + keyValue + " / " + lastKeyValue);
                }
            } while (true);

            //DebugPrint.println("IdxScanner seek to: " + keyValue);

            // seek the store heap to the next key
            // (this is what makes the scanner faster)
            super.storeHeap.seek(keyValue);
        }

        @Override
        public void close() {
            numberOfOngoingIndexedScans.decrementAndGet();
            keyProvider.close();

            super.close();
        }
    }

    class KeyProvider {
        private KeyValueHeap memstoreHeap;
        private final IdxSearchContext indexSearchContext;
        private final IntSet.IntSetIterator matchedExpressionIterator;
        private Scan scan;

        private KeyValue currentMemstoreKey = null;
        private KeyValue currentExpressionKey = null;
        private boolean exhausted = false;
        private byte[] startRow;
        private boolean isStartRowSatisfied;

        KeyProvider(IdxSearchContext idxSearchContext,
                    IntSet matchedExpression, Scan scan) {
            this.indexSearchContext = idxSearchContext;
            this.matchedExpressionIterator = matchedExpression.iterator();
            this.scan = scan;

            startRow = scan.getStartRow();
            isStartRowSatisfied = startRow == null;
        }

        /**
         * Must be called before the any method is accessed.
         */
        public void init() {
            memstoreHeap = initMemstoreHeap(scan);
        }

        private KeyValueHeap initMemstoreHeap(Scan scan) {
            List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
            // todo: can we reduce the cost of scanning the memory stores by
            // only using one entry from the family map

            for (byte[] family : regionInfo.getTableDesc().getFamiliesKeys()) {
                Store store = stores.get(family);
                scanners.addAll(getMemstoreScanners(store, scan.getStartRow()));
                //break;  // we only need one
            }
            return new KeyValueHeap(scanners.toArray(new KeyValueScanner[scanners.size()]), comparator);
        }

        /*
        * @return List of scanners ordered properly.
        */

        private List<KeyValueScanner> getMemstoreScanners(Store store, byte[] startRow) {
            List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
            // this seems a bit pointless because the memstore only ever returns an
            // array with only one element, but just incase...
            KeyValueScanner[] memstorescanners = store.memstore.getScanners();
            // to make sure we don't provide rows that the scan is not interested in
            // we seekTo the scan's startRow
            KeyValue seekTo = KeyValue.createFirstOnRow(startRow);
            for (int i = memstorescanners.length - 1; i >= 0; i--) {
                memstorescanners[i].seek(seekTo);
                scanners.add(memstorescanners[i]);
            }

            return scanners;
        }

        public KeyValue next() throws IOException {
            if (exhausted) return null;

            KeyValue currentKey;
            /*
              If either the current memstore or expression row is null get the next.
              Note: the row will be nulled when it's consumed.
            */
            if (currentMemstoreKey == null)
                currentMemstoreKey = nextMemstoreRow();
            if (currentExpressionKey == null)
                currentExpressionKey = nextExpressionRow();

            if (currentMemstoreKey == null && currentExpressionKey == null) {
                exhausted = true;
                // if both rows are null then the scanner is done
                return null;
            } else if (currentMemstoreKey == null) {
                currentKey = currentExpressionKey;
                currentExpressionKey = null;
            } else if (currentExpressionKey == null) {
                currentKey = currentMemstoreKey;
                currentMemstoreKey = null;
            } else {
                // determine which of the two keys is smaller (before the other) so that
                // the scan is processed in-order
                int comparisonResult = comparator.compareRows(currentMemstoreKey, currentExpressionKey);

                if (comparisonResult == 0) {
                    // if the two rows are equal then we'll use the memstore and clear the
                    // current expression row so that the next invocation progresses...
                    currentExpressionKey = null;
                    currentKey = currentMemstoreKey;
                } else if (comparisonResult < 0) {
                    currentKey = currentMemstoreKey;
                    currentMemstoreKey = null;
                } else { // if (comparisonResult > 0)
                    currentKey = currentExpressionKey;
                    currentExpressionKey = null;
                }
            }

            return currentKey;
        }

        private KeyValue nextExpressionRow() {
            KeyValue nextExpressionKey = null;
            while (matchedExpressionIterator.hasNext()) {
                int index = matchedExpressionIterator.next();
                nextExpressionKey = indexSearchContext.lookupRow(index);

                // if the scan has specified a startRow we need to keep looping
                // over the keys returned from the index until it's satisfied
                if (!isStartRowSatisfied) {
                    if (comparator.compareRows(nextExpressionKey, startRow) >= 0) {
                        isStartRowSatisfied = true;
                        break;
                    }
                } else {
                    break;
                }
            }

            return nextExpressionKey;
        }

        private KeyValue nextMemstoreRow() {
            /*
           This may appear a little expensive but the initial version is not concerned
           with the performance of the memstore.
            */
            final KeyValue firstOnNextRow = this.memstoreHeap.next();
            KeyValue nextKeyValue = this.memstoreHeap.peek();
            while (firstOnNextRow != null && nextKeyValue != null &&
                    comparator.compareRows(firstOnNextRow, nextKeyValue) == 0) {
                // progress to the next row
                // todo: is there a faster way of doing this?
                this.memstoreHeap.next();
                nextKeyValue = this.memstoreHeap.peek();
            }

            return firstOnNextRow;
        }

        /**
         * Close this key provider - delegate close and free memory.
         */
        public void close() {
            this.indexSearchContext.close();
            this.memstoreHeap.close();
        }
    }

    @Override
    public long heapSize() {
        return FIXED_OVERHEAD + super.heapSize() +
                indexManager.heapSize() + expressionEvaluator.heapSize();
    }

}
