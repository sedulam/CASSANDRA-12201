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

import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * This class serves the sole purpose of returning a BurstHourCompactionWriter
 * instead of a DefaultCompactionWriter, which is returned by the superclass, CompactionTask.
 */
public class BurstHourCompactionTask extends CompactionTask
{
    private final long sstableMaxSize;

    public BurstHourCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn,
                                   int gcBefore, long sstableMaxSize)
    {
        super(cfs, txn, gcBefore);
        this.sstableMaxSize = sstableMaxSize;
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                          Directories directories,
                                                          LifecycleTransaction transaction,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        return new MaxSSTableSizeWriter(cfs, directories, transaction, nonExpiredSSTables, sstableMaxSize, 0);
    }
}
