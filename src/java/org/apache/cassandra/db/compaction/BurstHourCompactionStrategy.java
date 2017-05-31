/*
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

package org.apache.cassandra.db.compaction;

import java.time.LocalTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;

import static com.google.common.collect.Iterables.filter;

/**
 * This strategy tries to take advantage of periods of the day where there's less I/O.
 * Full description can be found at CASSANDRA-12201.
 * For BHCS the minimum threshold is the minimum number of tables references for a key, that triggers its compaction.
 * The maximum threshold has the same purpose from the other strategies, i.e., the maximum of number tables that we
 * want to compact in each strategy run.
 */
public class BurstHourCompactionStrategy extends AbstractCompactionStrategy
{
    private volatile int estimatedRemainingTasks = 0;
    private final Set<SSTableReader> sstables = new HashSet<>();
    private static final Logger logger = LoggerFactory.getLogger(BurstHourCompactionStrategy.class);
    final BurstHourCompactionStrategyOptions bhcsOptions;
    /**
     * Controls the several threads that look for keys repeated in several SSTables. When we've reached the
     * maximum threshold for this CFS, this variable will be set to true, and all the threads will stop.
     */
    private volatile AtomicBoolean stopSearching;

    /**
     * @param cfs the {@link ColumnFamilyStore} that will be using this compaction strategy.
     * @param options the configuration optios chosen for this instance, if any
     */
    @SuppressWarnings("WeakerAccess")
    public BurstHourCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        bhcsOptions = new BurstHourCompactionStrategyOptions(options);
    }

    /**
     * Check every key of every table, until we've hit the threshold for SSTables with key repetitions.
     *
     * @return a set of tables have keys repeated among them
     */
    private Set<SSTableReader> getKeyReferences()
    {
        logger.info("Starting Burst Compaction analysis in CFS <" + cfs.getTableName() + ">.");

        Iterable<SSTableReader> candidates = filterSuspectSSTables(filter(cfs.getUncompactingSSTables(), sstables::contains));

        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();


        // Initiate variables for parallel threads start
        ExecutorService executor = Executors.newCachedThreadPool();
        Set<Future> threads = new HashSet<>();
        Set<Future> finishedThreads = new HashSet<>();
        Set<SSTableReader> ssTablesToCompact = new HashSet<>();
        /* Because candidates is an iterable, we don't know how many candidates there are, which is required to
        calculate the number of remaining compaction tasks, hence we use numberOfCandidates variable to finish the
        counting of the set
         */
        int numberOfCandidates = 0;
        stopSearching = new AtomicBoolean(false);

        /* Start one thread per each candidate SSTable. Each thread will find all the other tables that also have
         a key existing on this thread's table
          */
        for (SSTableReader ssTableReader : candidates)
        {
            if (!stopSearching.get())
            {
                KeyIterator keyIterator = new KeyIterator(ssTableReader.descriptor, cfs.metadata());

                Thread callable = new SSTableReferencesSearcher(candidates, keyIterator, minThreshold, maxThreshold, ssTablesToCompact, this.stopSearching);
                Future future = executor.submit(callable);
                threads.add(future);

                checkIfAllThreadsAreDone(threads, finishedThreads);
            }
            numberOfCandidates++;
        }

        // Wait for the maximum threshold to be reached, or until all the threads have completed
        while (!stopSearching.get())
        {
            checkIfAllThreadsAreDone(threads, finishedThreads);
        }

        estimatedRemainingTasks = numberOfCandidates / maxThreshold;

        logger.info("Number of remaining compaction tasks for CFS <" + cfs.getTableName() + ">: " + estimatedRemainingTasks);
        logger.info("BHCS analysis complete. Will compact " + ssTablesToCompact.size());

        return ssTablesToCompact;
    }

    /**
     * Checks to see if every thread has finished the search. If they did, change the
     * {@link BurstHourCompactionStrategy#stopSearching} control variable to true.
     * @param threads all the created search threads
     * @param finishedThreads all the threads that are done
     */
    private void checkIfAllThreadsAreDone(Set<Future> threads, Set<Future> finishedThreads)
    {
        for (Future thread : threads)
        {
            if (thread.isDone() && !finishedThreads.contains(thread))
            {
                finishedThreads.add(thread);
            }
        }

        if ((threads.size() - finishedThreads.size()) <= 0)
        {
            stopSearching.set(true);
        }
    }

    /**
     * In {@link BurstHourCompactionStrategy} if the current {@link LocalTime#now()} is outside of the time interval
     * configured to run the compaction, this method will always return null.
     *
     * @param gcBefore throw away tombstones older than this
     * @return the next background/minor compaction task to run; null if nothing to do.
     * <p>
     * Is responsible for marking its sstables as compaction-pending.
     */
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        LocalTime now = LocalTime.now();
        boolean isBurstHour = now.isAfter(bhcsOptions.getStartTime()) && now.isBefore(bhcsOptions.getEndTime());
        if (!isBurstHour)
        {
            return null;
        }

        Set<SSTableReader> ssTablesToCompact = getKeyReferences();
        return createBhcsCompactionTask(ssTablesToCompact, gcBefore);
    }

    /**
     * Creates the compaction task object that will return the implementation of
     * {@link org.apache.cassandra.db.compaction.writers.CompactionAwareWriter} used by
     * {@link BurstHourCompactionStrategy}.
     *
     * @param tables   the tables we want to compact
     * @param gcBefore throw away tombstones older than this
     * @return a compaction task object which will be later used to run the compaction per se
     */
    private AbstractCompactionTask createBhcsCompactionTask(Collection<SSTableReader> tables, int gcBefore)
    {
        if (tables.size() == 0)
        {
            return null;
        }
        else
        {
            LifecycleTransaction transaction = cfs.getTracker().tryModify(tables, OperationType.COMPACTION);
            return new BurstHourCompactionTask(cfs, transaction, gcBefore, bhcsOptions.sstableMaxSize * 1024L * 1024L);
        }
    }

    /**
     * Generate compaction tasks up to a point where no key is present in more than the minimum threshold of tables.
     * @param gcBefore    throw away tombstones older than this
     * @param splitOutput it's not relevant for this strategy, so the parameter will be ignored.
     * @return a compaction task that should be run to compact this columnfamilystore
     * as much as possible.  Null if nothing to do.
     * <p>
     * Is responsible for marking its sstables as compaction-pending.
     */
    public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        Set<AbstractCompactionTask> tasks = new HashSet<>(1);

        // estimatedRemainingTasks might be zero when we run for the first time, so we force the first run
        boolean firstIteration = true;

        while (firstIteration || estimatedRemainingTasks != 0)
        {
            if (firstIteration)
            {
                firstIteration = false;
            }

            Set<SSTableReader> ssTablesToCompact = getKeyReferences();

            AbstractCompactionTask task = createBhcsCompactionTask(ssTablesToCompact, gcBefore);
            if (task != null)
            {
                tasks.add(task);
            }
        }

        if (tasks.size() == 0)
        {
            return null;
        }
        else
        {
            return tasks;
        }
    }

    /**
     * Creates a compaction task based that will compact the SSTables in {@code sstables}.
     *
     * @param sstables SSTables to compact. Must be marked as compacting.
     * @param gcBefore throw away tombstones older than this
     * @return a compaction task corresponding to the requested sstables.
     * Will not be null. (Will throw if user requests an invalid compaction.)
     * <p>
     * Is responsible for marking its sstables as compaction-pending.
     */
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        assert !sstables.isEmpty();

        return createBhcsCompactionTask(sstables, gcBefore);
    }

    /**
     * @return the number of background tasks estimated still needed for this CFS
     */
    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    /**
     * @return size in bytes of the largest sstables compacted with this strategy
     */
    public long getMaxSSTableBytes()
    {
        return bhcsOptions.sstableMaxSize * 1024L * 1024L;
    }

    /**
     * When this strategy runs, every uncompacting table for this CFS, that is also included in
     *
     * {@link BurstHourCompactionStrategy#sstables}, will be consideredd a candidate for compaction.
     * @param added table that might be a candidate for compaction
     */
    public void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    /**
     * Remove {@code sstable} from the list of possible candidates for compaction on this CFS.
     *
     * @param sstable table to be excluded
     */
    public void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

    /**
     * Returns the sstables managed by this strategy instance
     */
    protected Set<SSTableReader> getSSTables()
    {
        return ImmutableSet.copyOf(sstables);
    }

    /**
     * Check if the options and values chosen for this compaction strategy are valid.
     *
     * @param options map of options chosen
     * @return the validated map of options. Empty if all the options were valid, not-empty otherwise
     * @throws ConfigurationException if any of the chosen values are not valid
     */
    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        // First validate all the options that are common to all the compaction strategies
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        // Validate options specific to BHCS
        uncheckedOptions = BurstHourCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        return uncheckedOptions;
    }

    /**
     * Used to search references in other tables for each key returned by {@link SSTableReferencesSearcher#keyIterator}.
     * Only keys with a multiplicity of at least {@link SSTableReferencesSearcher#minThreshold} will be considered for
     * compaction. When the number of gathered tables in {@link SSTableReferencesSearcher#ssTablesWithReferences}
     * reaches a minimum of {@link SSTableReferencesSearcher#maxThreshold}, the
     * {@link SSTableReferencesSearcher#stopSearching} control variable will be set to true, triggering the termination
     * of all active search threads.
     */
    private static class SSTableReferencesSearcher extends Thread
    {
        private final Iterable<SSTableReader> uncompactingSsTables;
        private final int minThreshold;
        private final Set<SSTableReader> ssTablesWithReferences;
        private final KeyIterator keyIterator;
        private final AtomicBoolean stopSearching;
        private final int maxThreshold;

        /**
         * Create one thread to search key multiplicity of every key in {@code keyIterator}.
         *
         * @param uncompactingSsTables the candidates for this compaction
         * @param keyIterator the keys to search for copies in other {@link SSTableReader}
         * @param minThreshold the minimum of key multiplicity that will trigger the compaction of the tables with
         *                     copies
         * @param maxThreshold the maximum number of tables that we can accept for each compaction task
         * @param ssTablesWithReferences the set that's acccepting SSTables for compaction from every thread
         * @param stopSearching the control variable for all the search threads. Volatile, with atomic access.
         */
        private SSTableReferencesSearcher(Iterable<SSTableReader> uncompactingSsTables,
                                          KeyIterator keyIterator, int minThreshold, int maxThreshold, Set<SSTableReader> ssTablesWithReferences,
                                          AtomicBoolean stopSearching)
        {
            this.keyIterator = keyIterator;
            this.uncompactingSsTables = uncompactingSsTables;
            this.minThreshold = minThreshold;
            this.ssTablesWithReferences = ssTablesWithReferences;
            this.stopSearching = stopSearching;
            this.maxThreshold = maxThreshold;
        }

        /**
         * Starts the search for copies of the keys in {@link SSTableReferencesSearcher#keyIterator}.
         */
        public void run()
        {
            while (keyIterator.hasNext() && !stopSearching.get())
            {
                DecoratedKey key = keyIterator.next();
                Set<SSTableReader> keyReferences = new HashSet<>();

                for (SSTableReader ssTable : uncompactingSsTables)
                {
                    // check if the key actually exists in this sstable, without updating cache and stats
                    if (ssTable. getPosition(key, SSTableReader.Operator.EQ, false) != null)
                    {
                        keyReferences.add(ssTable);
                    }
                }

                synchronized (stopSearching)
                {
                    if (!stopSearching.get())
                    {
                        if (keyReferences.size() >= minThreshold)
                        {
                            /* This might put more than the maxThreshold of tables in the set, however, if that's the
                            case no other table will be added after these.
                             */
                            ssTablesWithReferences.addAll(keyReferences);
                        }

                        if (ssTablesWithReferences.size() >= maxThreshold)
                        {
                            stopSearching.set(true);
                            break;
                        }
                    }
                }
            }

            keyIterator.close();
        }
    }
}
