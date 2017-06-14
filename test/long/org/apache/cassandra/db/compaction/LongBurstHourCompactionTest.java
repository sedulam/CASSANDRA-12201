/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.compaction;

import java.util.*;

import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.Util;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableUtils;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertEquals;

public class LongBurstHourCompactionTest
{
    private static final String KEYSPACE1 = "Keyspace1";
    private static final String CF_STANDARD = "Standard1";
    private static final String sizeOptionkey = "sstable_max_size";
    private static final String sizeOptionValue = "30";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();

        Map<String, String> options = new HashMap<>();
        options.put(CompactionParams.Option.MIN_THRESHOLD.toString(),
                    Integer.toString(CompactionParams.DEFAULT_MIN_THRESHOLD));
        options.put(CompactionParams.Option.MAX_THRESHOLD.toString(),
                    Integer.toString(CompactionParams.DEFAULT_MAX_THRESHOLD));
        options.put(sizeOptionkey, sizeOptionValue);

        CompactionParams compactionParams = CompactionParams.bhcs(options);

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD)
                                                .compaction(compactionParams));
    }

    @Before
    public void cleanupFiles()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");
        cfs.truncateBlocking();
    }

    /**
     * Test compaction with a very wide row.
     */
    @Test
    public void testCompactionWide() throws Exception
    {
        testCompaction(5, 5, 200000);
    }

    /**
     * Test compaction with lots of skinny rows.
     */
    @Test
    public void testCompactionSkinny() throws Exception
    {
        testCompaction(5, 200000, 1);
    }

    /**
     * Test compaction with lots of small sstables.
     */
    @Test
    public void testCompactionMany() throws Exception
    {
        testCompaction(100, 800, 5);
    }

    private void testCompaction(int sstableCount, int partitionsPerSSTable, int rowsPerPartition) throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");

        long maxTimestampExpected = Long.MIN_VALUE;
        for (int k = 0; k < sstableCount; k++)
        {
            SortedMap<String, PartitionUpdate> rows = new TreeMap<>();
            for (int j = 0; j < partitionsPerSSTable; j++)
            {
                String key = String.valueOf(j);
                // last sstable has highest timestamps
                UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), String.valueOf(j))
                                                     .withTimestamp(k);
                for (int i = 0; i < rowsPerPartition; i++)
                {
                    builder.newRow(String.valueOf(i)).add("val", String.valueOf(i));
                }
                rows.put(key, builder.build());
            }
            Descriptor descriptor = cfs.newSSTableDescriptor(cfs.getDirectories().getDirectoryForNewSSTables());
            Collection<SSTableReader> readers = SSTableUtils.prepare().dest(descriptor).write(rows);
            cfs.addSSTables(readers);
            maxTimestampExpected = Math.max(k, maxTimestampExpected);
        }

        // assert BEFORE compaction
        assertEquals(partitionsPerSSTable, Util.getAll(Util.cmd(cfs).build()).size());

        // give garbage collection a bit of time to catch up
        Thread.sleep(1000);

        forceCompactions(cfs, partitionsPerSSTable, maxTimestampExpected);
    }

    private void forceCompactions(ColumnFamilyStore cfs, long partitionsPerSSTable, long maxTimestampExpected)
    {
        // loop submitting parallel compactions until they all return 0
        do
        {
            CompactionManager.instance.performMaximal(cfs, false);
        } while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0);

        // assert AFTER compaction
        assertEquals(partitionsPerSSTable, Util.getAll(Util.cmd(cfs).build()).size());

        // make sure max timestamp of compacted sstables is recorded properly after compaction.
        CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpected);
        cfs.truncateBlocking();
    }
}
