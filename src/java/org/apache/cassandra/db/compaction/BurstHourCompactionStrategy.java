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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Pair;

/**
 * This strategy tries to take advantage of periods of the day where there's less I/O.
 * Full description can be found at CASSANDRA-12201.
 */
//
public class BurstHourCompactionStrategy extends AbstractCompactionStrategy
{
    private volatile int estimatedRemainingTasks;
    //TODO do we really need this variable?
    private final Set<SSTableReader> sstables = new HashSet<>();
    //TODO add logging
    private static final Logger logger = LoggerFactory.getLogger(BurstHourCompactionStrategy.class);
    private BurstHourCompactionStrategyOptions bhcsOptions;

    public BurstHourCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        estimatedRemainingTasks = 0;
        bhcsOptions = new BurstHourCompactionStrategyOptions(options);
    }


    private Map<DecoratedKey, Pair<String, Set<SSTableReader>>> getAllKeyReferences()
    {
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        Iterable<SSTableReader> candidates = filterSuspectSSTables(cfs.getUncompactingSSTables());

        // Get all the keys and the corresponding SSTables in which they exist
        Map<DecoratedKey, Pair<String, Set<SSTableReader>>> keyToTablesMap = new HashMap<>();
        outer: for(SSTableReader ssTable : candidates)
        {
            try(KeyIterator keyIterator = new KeyIterator(ssTable.descriptor, cfs.metadata()))
            {
                while (keyIterator.hasNext())
                {
                    DecoratedKey partitionKey = keyIterator.next();

                    Pair<String, Set<SSTableReader>> references;
                    Set<SSTableReader> ssTablesWithThisKey;
                    if (keyToTablesMap.containsKey(partitionKey))
                    {
                        references = keyToTablesMap.get(partitionKey);
                        ssTablesWithThisKey = references.right;
                    }
                    else
                    {
                        ssTablesWithThisKey = new HashSet<>();
                        references = Pair.create(ssTable.getColumnFamilyName(), ssTablesWithThisKey);
                        keyToTablesMap.put(partitionKey, references);
                    }

                    if (ssTablesWithThisKey.size() < maxThreshold)
                    {
                        ssTablesWithThisKey.add(ssTable);
                    }
                    else
                    {
                        //Reached maximum number of SSTables to compact for a given key
                        break outer;
                    }
                }
            }
        }

        return keyToTablesMap;
    }

    /**
     * Filter out the keys that are in less than referenced_sstable_limit SSTables
     * @return map with SSTables that share the same partition key more than referenced_sstable_limit amount
     */
    private Map<String, Set<SSTableReader>> removeColdBuckets(Map<DecoratedKey, Pair<String, Set<SSTableReader>>> allReferences)
    {
        int minThreshold = cfs.getMinimumCompactionThreshold();

        Map<String, Set<SSTableReader>> keyCountAboveThreshold = new HashMap<>();

        for(Map.Entry<DecoratedKey, Pair<String, Set<SSTableReader>>> entry : allReferences.entrySet())
        {
            Pair<String, Set<SSTableReader>> keyReferences = entry.getValue();
            if (keyReferences.right.size() >= minThreshold)
            {
                String tableName = keyReferences.left;
                if (keyCountAboveThreshold.containsKey(tableName))
                {
                    // Because we're using a set, duplicates won't be an issue
                    keyCountAboveThreshold.get(tableName).addAll(keyReferences.right);
                }
                else
                {
                    Set<SSTableReader> ssTablesSet = new HashSet<>();
                    ssTablesSet.addAll(keyReferences.right);
                    keyCountAboveThreshold.put(tableName, ssTablesSet);
                }
            }
        }

        return keyCountAboveThreshold;
    }

    private Set<SSTableReader> selectHottestBucket(Map<String, Set<SSTableReader>> allBuckets)
    {
        long maxReferences = 0;
        Set<SSTableReader> hottestSet = null;

        for(Set<SSTableReader> set: allBuckets.values())
        {
            long setReferences = set.size();
            if (setReferences > maxReferences)
            {
                maxReferences = setReferences;
                hottestSet = set;
            }
        }

        return hottestSet;
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

        Set<SSTableReader> ssTablesToCompact = getSSTables();
        return createBhcsCompactionTask(ssTablesToCompact, gcBefore);
    }

    /**
     * Creates the compaction task object.
     * @param tables the tables we want to compact
     * @param gcBefore throw away tombstones older than this
     * @return a compaction task object which will be later used to run the compaction per se
     */
    private AbstractCompactionTask createBhcsCompactionTask(Collection<SSTableReader> tables, int gcBefore)
    {
        if (tables.size() == 0){
            return null;
        }
        else {
            LifecycleTransaction transaction = cfs.getTracker().tryModify(tables, OperationType.COMPACTION);
            return new CompactionTask(cfs, transaction, gcBefore);
        }
    }

    /**
     * @param gcBefore    throw away tombstones older than this
     * @param splitOutput it's not relevant for this strategy because the strategy whole purpose to is always get a compaction as big as possible
     * @return a compaction task that should be run to compact this columnfamilystore
     * as much as possible.  Null if nothing to do.
     * <p>
     * Is responsible for marking its sstables as compaction-pending.
     */
    public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        Map<DecoratedKey, Pair<String, Set<SSTableReader>>> keyReferences = getAllKeyReferences();

        Map<String, Set<SSTableReader>> hotBuckets = removeColdBuckets(keyReferences);

        Set<AbstractCompactionTask> allTasks = new HashSet<>();

        for(Set<SSTableReader> ssTables : hotBuckets.values())
        {
            AbstractCompactionTask task = createBhcsCompactionTask(ssTables, gcBefore);
            allTasks.add(task);
        }

        return allTasks;
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
        Map<DecoratedKey, Pair<String, Set<SSTableReader>>> allReferences = getAllKeyReferences();

        Map<String, Set<SSTableReader>> hotBuckets = removeColdBuckets(allReferences);

        estimatedRemainingTasks = hotBuckets.size();

        return selectHottestBucket(hotBuckets);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = BurstHourCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        return uncheckedOptions;
    }
}
