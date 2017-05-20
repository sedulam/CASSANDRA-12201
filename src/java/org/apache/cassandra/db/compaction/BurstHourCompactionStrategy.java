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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
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
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * This strategy tries to take advantage of periods of the day where there's less I/O.
 * Full description can be found at CASSANDRA-12201.
 * For BHCS the minimum threshold is the minimum number of tables references for a key, that triggerss its compaction.
 * The maximum threshold has the same purpose from the other strategies, i.e., the maximum of number tables that we
 * can compact in each strategy run.
 */
public class BurstHourCompactionStrategy extends AbstractCompactionStrategy
{
    private volatile int estimatedRemainingTasks;
    private final Set<SSTableReader> sstables = new HashSet<>();
    private static final Logger logger = LoggerFactory.getLogger(BurstHourCompactionStrategy.class);
    private final BurstHourCompactionStrategyOptions bhcsOptions;
    private AtomicBoolean stopSearching;

    public BurstHourCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        estimatedRemainingTasks = 0;
        bhcsOptions = new BurstHourCompactionStrategyOptions(options);
    }

    /**
     * Check every key of every table, until we've hit the threshold for SSTables with key repetitions.
     *
     * @return a set of tables that share a set of the same keys
     */
    private Set<SSTableReader> getKeyReferences()
    {
        logger.info("Starting Burst Compaction analysis in CFS <" + cfs.getTableName() + ">.");

        Iterable<SSTableReader> candidates = filterSuspectSSTables(sstables);

        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        Set<SSTableReader> ssTablesToCompact = new HashSet<>();

        // Get all the keys and the corresponding SSTables in which they exist
        ExecutorService executor = Executors.newCachedThreadPool();
        Set<Future<Set<SSTableReader>>> threads = new HashSet<>();
        Set<Future> finishedThreads = new HashSet<>();

        /* because candidates is an iterable, we don't know its size, which is required to calculate the number of
        remaining compaction tasks, hence we use this maxThresholdReached and numberOfCandidates variables to finish the counting of the set
         */
        int numberOfCandidates = 0;
        stopSearching = new AtomicBoolean(false);
        for (SSTableReader ssTableReader : candidates)
        {
            if (!stopSearching.get())
            {
                logger.info("Searching table " + ssTableReader.getFilename());

                KeyIterator keyIterator = new KeyIterator(ssTableReader.descriptor, cfs.metadata());

                System.out.println("KeyIterator created on SSTable: " + ssTableReader.getFilename());

                Callable<Set<SSTableReader>> callable = new SSTableReferencesSearcher(candidates, keyIterator, maxThreshold, ssTablesToCompact, this.stopSearching);
                Future<Set<SSTableReader>> future = executor.submit(callable);
                threads.add(future);

                checkIfStopSearch(ssTablesToCompact, maxThreshold, threads, finishedThreads, minThreshold);
            }

            numberOfCandidates++;
        }

        while (!stopSearching.get())
        {
            checkIfStopSearch(ssTablesToCompact, maxThreshold, threads, finishedThreads, minThreshold);
        }

        logger.info("BHCS analysis complete. Will compact " + ssTablesToCompact.size());

        estimatedRemainingTasks = numberOfCandidates / cfs.getMaximumCompactionThreshold();
        logger.info("Number of remaining compaction tasks for CFS <" + cfs.getTableName() + ">: " + estimatedRemainingTasks);

        return ssTablesToCompact;
    }

    private void checkIfStopSearch(Set<SSTableReader> ssTablesToCompact, int maxThreshold, Set<Future<Set<SSTableReader>>> threads, Set<Future> finishedThreads, int minThreshold)
    {
        if (ssTablesToCompact.size() >= maxThreshold)
        {
            stopSearching.set(true);
        }

        boolean allThreadsDone = checkFinishedThreads(threads, finishedThreads, ssTablesToCompact, minThreshold);
        if (allThreadsDone)
        {
            stopSearching.set(true);
        }
    }

    private static boolean checkFinishedThreads(Set<Future<Set<SSTableReader>>> threads, Set<Future> finishedThreads,
                                                Set<SSTableReader> ssTablesToCompact,
                                                int minThreshold)
    {
        for (Future<Set<SSTableReader>> thread : threads)
        {
            if (thread.isDone() && !finishedThreads.contains(thread))
            {
                finishedThreads.add(thread);
                try
                {
                    Set<SSTableReader> references = thread.get();
                    if (references.size() >= minThreshold)
                    {
                        logger.info("Finished searching one of the candidates tables. Cross references with "
                                    + references.size() + " tables found.");
                        ssTablesToCompact.addAll(references);

                        break;
                    }
                }
                catch (InterruptedException | ExecutionException e)
                {
                    logger.error("One of the threads responsible for finding key references terminated unexpectadly", e);
                }
            }
        }

        if ((threads.size() - finishedThreads.size()) > 0)
        {
            logger.info("Still need to finish " + (threads.size() - finishedThreads.size()) + " threads.");
            return false;
        }
        else
        {
            return true;
        }
    }

    /**
     * @param gcBefore throw away tombstones older than this
     * @return the next background/minor compaction task to run; null if nothing to do.
     * <p>
     * Is responsible for marking its sstables as compaction-pending.
     */
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        LocalTime now = LocalTime.now();
        boolean isBurstHour = now.isAfter(bhcsOptions.startTime) && now.isBefore(bhcsOptions.endTime);
        if (!isBurstHour)
        {
            return null;
        }

        Set<SSTableReader> ssTablesToCompact = getKeyReferences();
        return createBhcsCompactionTask(ssTablesToCompact, gcBefore);
    }

    /**
     * Creates the compaction task object.
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
     * @param gcBefore    throw away tombstones older than this
     * @param splitOutput it's not relevant for this strategy, so the parameter will be ignored.
     * @return a compaction task that should be run to compact this columnfamilystore
     * as much as possible.  Null if nothing to do.
     * <p>
     * Is responsible for marking its sstables as compaction-pending.
     */
    public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        Set<SSTableReader> ssTablesToCompact = getKeyReferences();
        if (ssTablesToCompact.size() > 0)
        {
            Set<AbstractCompactionTask> tasks = new HashSet<>(1);
            tasks.add(createBhcsCompactionTask(ssTablesToCompact, gcBefore));
            return tasks;
        }
        else
        {
            return null;
        }
    }

    /**
     * @param sstables SSTables to compact. Must be marked as compacting.
     * @param gcBefore throw away tombstones older than this
     * @return a compaction task corresponding to the requested sstables.
     * Will not be null. (Will throw if user requests an invalid compaction.)
     * <p>
     * Is responsible for marking its sstables as compaction-pending.
     */
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        return createBhcsCompactionTask(sstables, gcBefore);
    }

    /**
     * @return the number of background tasks estimated to still be needed for this columnfamilystore
     */
    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    /**
     * @return size in bytes of the largest sstables for this strategy
     */
    public long getMaxSSTableBytes()
    {
        return bhcsOptions.sstableMaxSize * 1024L * 1024L;
    }

    public void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

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

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = BurstHourCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        return uncheckedOptions;
    }


    private static class SSTableReferencesSearcher implements Callable<Set<SSTableReader>>
    {
        private final Iterable<SSTableReader> uncompactingSsTables;
        private final int maxThreshold;
        private final Set<SSTableReader> ssTablesWithReferences;
        private final KeyIterator keyIterator;
        private final AtomicBoolean stopSearching;

        private SSTableReferencesSearcher(Iterable<SSTableReader> uncompactingSsTables,
                                          KeyIterator keyIterator, int maxThreshold, Set<SSTableReader> ssTablesWithReferences,
                                          AtomicBoolean stopSearching)
        {
            this.keyIterator = keyIterator;
            this.uncompactingSsTables = uncompactingSsTables;
            this.maxThreshold = maxThreshold;
            this.ssTablesWithReferences = ssTablesWithReferences;
            this.stopSearching = stopSearching;
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        public Set<SSTableReader> call() throws Exception
        {
            while (keyIterator.hasNext() && !stopSearching.get())
            {
                DecoratedKey key = keyIterator.next();

                logger.info("Starting scan for key " + key.toString());

                for (SSTableReader ssTable : uncompactingSsTables)
                {
                    // check if the key actually exists in this sstable, without updating cache and stats
                    if (ssTable.getPosition(key, SSTableReader.Operator.EQ, false) != null)
                    {
                        ssTablesWithReferences.add(ssTable);
                    }
                }

                logger.info("Key " + key.toString() + " is referenced by " + ssTablesWithReferences.size() + " tables.");

                if (ssTablesWithReferences.size() >= maxThreshold)
                {
                    break;
                }
            }

            keyIterator.close();

            return ssTablesWithReferences;
        }
    }
}
