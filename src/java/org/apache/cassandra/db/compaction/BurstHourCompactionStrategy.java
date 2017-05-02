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
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.exceptions.CompactionException;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import static com.google.common.collect.Iterables.filter;

/**
 * This strategy tries to take advantage of periods of the day where there's less I/O.
 * Full description can be found at CASSANDRA-12201.
 */
public class BurstHourCompactionStrategy extends AbstractCompactionStrategy
{
    private volatile int estimatedRemainingTasks;
    private int referenced_sstable_limit = 3;
    private final Set<SSTableReader> sstables = new HashSet<>();
    private static final Logger logger = LoggerFactory.getLogger(BurstHourCompactionStrategy.class);

    public BurstHourCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        estimatedRemainingTasks = 0;
    }

    private Map<DecoratedKey, Pair<String, Set<SSTableReader>>> getAllKeyReferences()
    {
        Iterable<SSTableReader> candidates = filterSuspectSSTables(cfs.getUncompactingSSTables());

        // Get all the keys and the corresponding SSTables in which they exist
        Map<DecoratedKey, Pair<String, Set<SSTableReader>>> keyToTablesMap = new HashMap<>();
        for(SSTableReader ssTable : candidates){
            try(KeyIterator keyIterator = new KeyIterator(ssTable.descriptor, cfs.metadata))
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

                    if (ssTablesWithThisKey != null)
                    {
                        ssTablesWithThisKey.add(ssTable);
                    }
                    else
                    {
                        throw new CompactionException(ExceptionCode.SERVER_ERROR, "SSTables reference set cannot be null at this point");
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
        Map<String, Set<SSTableReader>> keyCountAboveThreshold = new HashMap<>();

        for(Map.Entry<DecoratedKey, Pair<String, Set<SSTableReader>>> entry : allReferences.entrySet())
        {
            Pair<String, Set<SSTableReader>> keyReferences = entry.getValue();
            if (keyReferences.right.size() >= referenced_sstable_limit)
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

    private Set<SSTableReader> gatherSSTablesToCompact(){

        Map<DecoratedKey, Pair<String, Set<SSTableReader>>> allReferences = getAllKeyReferences();

        Map<String, Set<SSTableReader>> hotBuckets = removeColdBuckets(allReferences);

        estimatedRemainingTasks = hotBuckets.size();

        return selectHottestBucket(hotBuckets);
    }

    /**
     * @param gcBefore throw away tombstones older than this
     * @return the next background/minor compaction task to run; null if nothing to do.
     * <p>
     * TODO does the following line still applies? If not, change the superclass doc
     * Is responsible for marking its sstables as compaction-pending.
     */
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        Set<SSTableReader> ssTablesToCompact = gatherSSTablesToCompact();

        if (ssTablesToCompact.size() == 0){
            return null;
        }
        else {
            LifecycleTransaction transaction = cfs.getTracker().tryModify(ssTablesToCompact, OperationType.COMPACTION);
            return new CompactionTask(cfs, transaction, gcBefore);
        }
    }

    /**
     * @param gcBefore    throw away tombstones older than this
     * @param splitOutput it's not relevant for this strategy
     * @return a compaction task that should be run to compact this columnfamilystore
     * as much as possible.  Null if nothing to do.
     * <p>
     * Is responsible for marking its sstables as compaction-pending.
     */
    public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        throw new NotImplementedException();
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
        throw new NotImplementedException();
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
}
