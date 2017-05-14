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

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * This strategy tries to take advantage of periods of the day where there's less I/O.
 * Full description can be found at CASSANDRA-12201.
 */
//
public class BurstHourCompactionStrategy extends AbstractCompactionStrategy
{
    //TODO implement this
    private volatile int estimatedRemainingTasks;
    //TODO do we really need this variable?
    private final Set<SSTableReader> sstables = new HashSet<>();
    //TODO add logging
    private static final Logger logger = LoggerFactory.getLogger(BurstHourCompactionStrategy.class);
    private final BurstHourCompactionStrategyOptions bhcsOptions;

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
        Iterable<SSTableReader> candidates = filterSuspectSSTables(cfs.getUncompactingSSTables());
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        Set<SSTableReader> ssTablesToCompact = new HashSet<>();

        // Get all the keys and the corresponding SSTables in which they exist
        ExecutorService executor = Executors.newCachedThreadPool();
        Set<Future> threads = new HashSet<>();
        Set<Future> finishedThreads = new HashSet<>();

        for (SSTableReader ssTableReader : candidates)
        {
            System.out.println("Searching table " + ssTableReader.getFilename());

            KeyIterator keyIterator = new KeyIterator(ssTableReader.descriptor, cfs.metadata());
            Callable callable = new SSTableReferencesSearcher(candidates, keyIterator, maxThreshold, ssTablesToCompact);
            Future<Set<SSTableReader>> future = executor.submit(callable);
            threads.add(future);

            checkFinishedThreads(threads, finishedThreads, ssTablesToCompact, minThreshold, maxThreshold);

            // If after searching a whole table we have a minimum of references, proceed to compact them
            // This is to prevent spending too much time in finding references.
            if (ssTablesToCompact.size() >= minThreshold)
            {
                break;
            }
        }

        while (ssTablesToCompact.size() < minThreshold)
        {
            boolean allThreadsDone = checkFinishedThreads(threads, finishedThreads, ssTablesToCompact, minThreshold, maxThreshold);

            if (allThreadsDone || ssTablesToCompact.size() >= maxThreshold)
            {
                break;
            }
        }

        //TODO shall I add trimming of ssTablesToCompact until the reaches the maximum threshold?

        return ssTablesToCompact;
    }

    private static boolean checkFinishedThreads(Set<Future> threads, Set<Future> finishedThreads,
                                                Set<SSTableReader> tablesWithRepeatedKeys,
                                                int minThreshold, int maxThreshold)
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
                        tablesWithRepeatedKeys.addAll(references);

                        if (tablesWithRepeatedKeys.size() >= maxThreshold)
                        {
                            terminateRemainingSearchThreads(threads, finishedThreads);
                            return true;
                        }
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
            System.out.println("Still need to finish " + (threads.size() - finishedThreads.size()) + " threads.");
            return false;
        }
        else
        {
            return true;
        }
    }

    private static void terminateRemainingSearchThreads(Set<Future> threads, Set<Future> finishedThreads)
    {
        for (Future thread : threads)
        {
            if (!finishedThreads.contains(thread))
            {
                thread.cancel(true);
                System.out.println("Thread " + thread.toString() + " terminated.");
            }
        }
    }

    /**
     * @param gcBefore throw away tombstones older than this
     * @return the next background/minor compaction task to run; null if nothing to do.
     * <p>
     * TODO does the following line still applies? If not, change the superclass doc. Repeat for other methods.
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
            return new CompactionTask(cfs, transaction, gcBefore);
        }
    }

    /**
     * @param gcBefore    throw away tombstones older than this
     * @param splitOutput TODO
     * @return a compaction task that should be run to compact this columnfamilystore
     * as much as possible.  Null if nothing to do.
     * <p>
     * Is responsible for marking its sstables as compaction-pending.
     */
    public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        Iterable keyReferences = cfs.getUncompactingSSTables();

        LifecycleTransaction txn = cfs.getTracker().tryModify(keyReferences, OperationType.COMPACTION);

        if (splitOutput)
        {
            //TODO
            throw new NotImplementedException();
        }
        else
        {
            Set<AbstractCompactionTask> tasks = new HashSet<>(1);
            tasks.add(new CompactionTask(cfs, txn, gcBefore));
            return tasks;
        }
    }

    /**
     * @param sstables SSTables to compact. Must be marked as compacting.
     * @param gcBefore throw away tombstones older than this
     * @return a compaction task corresponding to the requested sstables.
     * Will not be null. (Will throw if user requests an invalid compaction.)
     * <p>
     * Is responsible for marking its sstables as compaction-pending.
     * TODO DTS, STCS and now BHCS all do basically the same thing in this method. Wouldn't it be better to define this in the superclass and LCS would override it?
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
        //TODO why is every strategy, except for LCS, returing this value?
        return Long.MAX_VALUE;
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


    private class SSTableReferencesSearcher implements Callable
    {
        private final Iterable<SSTableReader> uncompactingSsTables;
        private final int maxThreshold;
        private final Set ssTablesWithReferences;
        private final KeyIterator keyIterator;

        private SSTableReferencesSearcher(Iterable<SSTableReader> uncompactingSsTables,
                                          KeyIterator keyIterator, int maxThreshold, Set ssTablesWithReferences)
        {
            this.keyIterator = keyIterator;
            this.uncompactingSsTables = uncompactingSsTables;
            this.maxThreshold = maxThreshold;
            this.ssTablesWithReferences = ssTablesWithReferences;
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        public Set<SSTableReader> call() throws Exception
        {
            while (keyIterator.hasNext())
            {
                DecoratedKey key = keyIterator.next();

                System.out.println("Starting scan for key " + key.toString());

                for (SSTableReader ssTable : uncompactingSsTables)
                {
                    // check if the key actually exists in this sstable, without updating cache and stats
                    if (ssTable.getPosition(key, SSTableReader.Operator.EQ, false) != null)
                    {
                        ssTablesWithReferences.add(ssTable);
                    }
                }

                System.out.println("Key " + key.toString() + " is referenced by " + ssTablesWithReferences.size() + " tables.");

                if (ssTablesWithReferences.size() >= maxThreshold)
                {
                    break;
                }
            }

            return ssTablesWithReferences;
        }
    }
}
