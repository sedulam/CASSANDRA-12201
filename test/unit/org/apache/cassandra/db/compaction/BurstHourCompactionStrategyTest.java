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

import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Iterables.filter;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Test class for {@link BurstHourCompactionStrategy}.
 */
public class BurstHourCompactionStrategyTest
{
    private static final String keyspaceName = "TestingBhcsKeyspace";
    private static final String tableName = "TestingBhcsTable";
    private static final String sizeOptionkey = "sstable_max_size";
    private static final String sizeOptionValue = "30";
    private static ColumnFamilyStore cfs;
    private static Set<BurstHourCompactionStrategy> strategies;
    private static int gcBefore;
    private static final Set<AbstractCompactionTask> tasks = new HashSet<>();

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

        SchemaLoader.createKeyspace(keyspaceName,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(keyspaceName, tableName)
                                                .compaction(compactionParams));
        cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);
        cfs.disableAutoCompaction();

        strategies = new HashSet<>();

        for (AbstractCompactionStrategy strategy : cfs.getCompactionStrategyManager().getUnrepaired())
        {
            strategies.add((BurstHourCompactionStrategy) strategy);
        }

        gcBefore = CompactionManager.getDefaultGcBefore(cfs, FBUtilities.nowInSeconds());
    }

    private void createData(int keyReferences)
    {
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b); // 100 KB value, make it easy to have multiple files

        int rowsCount = 10;

        Set<UpdateBuilder> rows = new HashSet<>();

        for (int r = 0; r < rowsCount; r++)
        {
            UpdateBuilder row = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            rows.add(row);
        }

        for(int c = 0; c < keyReferences; c++)
        {
            for(UpdateBuilder row : rows)
            {
                row.newRow("column" + c).add("val", value);
                row.applyUnsafe();
            }
            cfs.forceBlockingFlush();
        }

        while (true)
        {
            int tableCount = 0;
            for (SSTableReader ssTableReader : cfs.getUncompactingSSTables())
            {
                tableCount++;
            }
            if (tableCount == keyReferences)
            {
                break;
            }
        }
    }

    @After
    public void deleteData()
    {
        for(AbstractCompactionTask task : tasks)
        {
            if (task != null)
            {
                task.transaction.abort();
            }
        }
        tasks.clear();
        cfs.truncateBlocking();

        while (true)
        {
            int tableCount = 0;
            Set<SSTableReader> liveTables =  cfs.getLiveSSTables();
            for (SSTableReader ssTableReader : liveTables)
            {
                tableCount++;
            }
            if (tableCount == 0)
            {
                break;
            }
        }
    }

    @Test
    public void getNextBackgroundTaskOutsideBurstHour() throws Exception
    {
        createData(CompactionParams.DEFAULT_MIN_THRESHOLD + 1);
        LocalTime start = LocalTime.now().plusHours(6);
        LocalTime end = LocalTime.now().plusHours(8);
        setBurstHour(start, end);
        for(BurstHourCompactionStrategy strategy : strategies)
        {
            AbstractCompactionTask task = strategy.getNextBackgroundTask(gcBefore);
            tasks.add(task);
            assertNull(task);
        }
    }

    @Test
    public void getNextBackgroundTaskDuringBurstHour() throws Exception
    {
        createData(CompactionParams.DEFAULT_MIN_THRESHOLD + 1);
        LocalTime start = LocalTime.now().minusMinutes(30);
        LocalTime end = LocalTime.now().plusMinutes(30);
        setBurstHour(start, end);

        for(BurstHourCompactionStrategy strategy : strategies)
        {
            AbstractCompactionTask task = strategy.getNextBackgroundTask(gcBefore);
            tasks.add(task);
            assertNotNull(task);
        }
    }

    @Test
    public void getNextBackgroundTaskNotEnoughReferences() throws Exception
    {
        createData(CompactionParams.DEFAULT_MIN_THRESHOLD - 1);
        LocalTime start = LocalTime.now().minusMinutes(30);
        LocalTime end = LocalTime.now().plusMinutes(30);
        setBurstHour(start, end);

        for(BurstHourCompactionStrategy strategy : strategies)
        {
            AbstractCompactionTask task = strategy.getNextBackgroundTask(gcBefore);
            tasks.add(task);
            assertNull(task);
        }
    }

    @Test
    public void getMaximalTaskNotEnoughReferences() throws Exception
    {
        createData(CompactionParams.DEFAULT_MIN_THRESHOLD - 1);

        for(BurstHourCompactionStrategy strategy : strategies)
        {
            Collection<AbstractCompactionTask> maximalTaskList = strategy.getMaximalTask(gcBefore, true);
            if (maximalTaskList != null)
            {
                tasks.addAll(maximalTaskList);
            }
            assertNull(maximalTaskList);
        }
    }

    @Test
    public void getMaximalTask() throws Exception
    {
        createData(CompactionParams.DEFAULT_MIN_THRESHOLD + 1);

        for(BurstHourCompactionStrategy strategy : strategies)
        {
            Collection<AbstractCompactionTask> maximalTaskList = strategy.getMaximalTask(gcBefore, true);
            if (maximalTaskList != null)
            {
                tasks.addAll(maximalTaskList);
            }
            assertTrue(maximalTaskList != null);
            assertFalse(maximalTaskList.isEmpty());
        }
    }

    @Test
    public void getUserDefinedTask() throws Exception
    {
        createData(CompactionParams.DEFAULT_MIN_THRESHOLD);

        for(BurstHourCompactionStrategy strategy : strategies)
        {
            Iterable<SSTableReader> candidates = AbstractCompactionStrategy.filterSuspectSSTables(
                filter(cfs.getUncompactingSSTables(), strategy.getSSTables()::contains));
            Set<SSTableReader> tables = new HashSet<>();
            for (SSTableReader table : candidates)
            {
                tables.add(table);
            }
            AbstractCompactionTask task = strategy.getUserDefinedTask(tables, gcBefore);
            tasks.add(task);
            assertNotNull(task);
        }
    }

    @Test(expected = ConfigurationException.class)
    public void validateOptionsWrongValues() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put(BurstHourCompactionStrategyOptions.START_TIME_KEY, "WRONG VALUE");
        options.put(BurstHourCompactionStrategyOptions.SSTABLE_MAX_SIZE_KEY, "50");
        assertFalse(BurstHourCompactionStrategy.validateOptions(options).isEmpty());
    }

    @Test
    public void validateOptionsWrongOptions() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("Non-existing-key", "100");
        options.put(BurstHourCompactionStrategyOptions.SSTABLE_MAX_SIZE_KEY, "50");
        BurstHourCompactionStrategy.validateOptions(options);
    }

    @Test
    public void validateOptions() throws Exception
    {
        List<Pair<String, String>> pairs = new LinkedList<>();
        pairs.add(Pair.create(CompactionParams.Option.MIN_THRESHOLD.toString(), "7"));
        pairs.add(Pair.create(CompactionParams.Option.MAX_THRESHOLD.toString(), "50"));
        pairs.add(Pair.create(BurstHourCompactionStrategyOptions.START_TIME_KEY, "04:55:49"));
        pairs.add(Pair.create(BurstHourCompactionStrategyOptions.END_TIME_KEY, "14:34:13"));
        pairs.add(Pair.create(BurstHourCompactionStrategyOptions.SSTABLE_MAX_SIZE_KEY, "14"));

        for (Pair<String, String> pair : pairs)
        {
            Map<String, String> options = new HashMap<>();
            options.put(pair.left, pair.right);
            assertTrue(BurstHourCompactionStrategy.validateOptions(options).isEmpty());
        }
    }

    private void setBurstHour(LocalTime start, LocalTime end)
    {
        for(BurstHourCompactionStrategy compactionStrategy : strategies)
        {
            BurstHourCompactionStrategyOptions options = compactionStrategy.bhcsOptions;

            options.setStartTime(start);
            options.setEndTime(end);
        }
    }
}