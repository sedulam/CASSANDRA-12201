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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static com.google.common.collect.Iterables.filter;

/**
 * This strategy tries to take advantage of periods of the day where there's less I/O.
 * Full description can be found at CASSANDRA-12201.
 */
public class BurstHourCompactionStrategy extends AbstractCompactionStrategy
{
    private int referenced_sstable_limit = 3;
    private final Set<SSTableReader> sstables = new HashSet<>();

    public BurstHourCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
    }

    private Map<DecoratedKey, List<SSTableReader>> gatherSSTablesToCompact(){

        Iterable<SSTableReader> candidates = filterSuspectSSTables(filter(cfs.getUncompactingSSTables(), sstables::contains));

        Map<DecoratedKey, List<SSTableReader>> keyToTablesMap = new HashMap<>();

        for(SSTableReader ssTableReader : candidates){
            try(KeyIterator keyIterator = new KeyIterator(ssTableReader.descriptor, cfs.metadata))
            {
                while (keyIterator.hasNext())
                {
                    DecoratedKey key = keyIterator.next();

                    if (!keyToTablesMap.containsKey(key))
                    {
                        List<SSTableReader> tablesWithThisKey = new ArrayList<>();
                        tablesWithThisKey.add(ssTableReader);
                        keyToTablesMap.put(key, tablesWithThisKey);
                    }
                }
            }
        }

        // Filter out the keys that are in less than referenced_sstable_limit SSTables
        Map<DecoratedKey, List<SSTableReader>> keyCountAboveThreshold = new HashMap<>();
        for(DecoratedKey key: keyToTablesMap.keySet()){
            List<SSTableReader> keyReferences = keyCountAboveThreshold.get(key);
            if (keyReferences.size() >= referenced_sstable_limit){
                keyCountAboveThreshold.put(key, keyReferences);
            }
        }

        return keyCountAboveThreshold;
    }

    /**
     * @param gcBefore throw away tombstones older than this
     * @return the next background/minor compaction task to run; null if nothing to do.
     * <p>
     * Is responsible for marking its sstables as compaction-pending.
     */
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        return null;
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
        return null;
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
        return null;
    }

    /**
     * @return the number of background tasks estimated to still be needed for this columnfamilystore
     */
    public int getEstimatedRemainingTasks()
    {
        return 0;
    }

    /**
     * @return size in bytes of the largest sstables for this strategy
     */
    public long getMaxSSTableBytes()
    {
        return 0;
    }

    public void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    public void removeSSTable(SSTableReader sstable)
    {

    }
}
