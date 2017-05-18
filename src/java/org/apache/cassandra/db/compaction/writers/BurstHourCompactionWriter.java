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

package org.apache.cassandra.db.compaction.writers;

import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

/**
 * Created by Pedro Gordo on 16/05/17.
 */
public class BurstHourCompactionWriter extends CompactionAwareWriter
{
    private final long sstableMaxSize;
    private final long estimatedSSTables;
    private Directories.DataDirectory sstableDirectory;

    public BurstHourCompactionWriter(ColumnFamilyStore cfs, Directories directories,
                                     Set<SSTableReader> tablesToCompact, LifecycleTransaction transaction,
                                     long sstableMaxSize)
    {
        super(cfs, directories, transaction, tablesToCompact, false);
        this.sstableMaxSize = sstableMaxSize;

        long totalSize = MaxSSTableSizeWriter.getTotalWriteSize(nonExpiredSSTables, estimatedTotalKeys, cfs, txn.opType());
        estimatedSSTables = Math.max(1, totalSize / sstableMaxSize);
    }

    protected boolean realAppend(UnfilteredRowIterator partition)
    {
        RowIndexEntry rie = sstableWriter.append(partition);
        if (sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten() > sstableMaxSize)
        {
            switchCompactionLocation(sstableDirectory);
        }
        return rie != null;
    }

    /**
     * Implementations of this method should finish the current sstable writer and start writing to this directory.
     * <p>
     * Called once before starting to append and then whenever we see a need to start writing to another directory.
     *
     * @param directory
     */
    protected void switchCompactionLocation(Directories.DataDirectory directory)
    {
        sstableDirectory = directory;
        @SuppressWarnings("resource")
        SSTableWriter writer = SSTableWriter.create(cfs.newSSTableDescriptor(getDirectories().getLocationForDisk(sstableDirectory)),
                                                    estimatedTotalKeys / estimatedSSTables,
                                                    minRepairedAt,
                                                    pendingRepair,
                                                    cfs.metadata,
                                                    new MetadataCollector(txn.originals(), cfs.metadata().comparator, 0),
                                                    SerializationHeader.make(cfs.metadata(), nonExpiredSSTables),
                                                    cfs.indexManager.listIndexes(),
                                                    txn);

        sstableWriter.switchWriter(writer);
    }
}
