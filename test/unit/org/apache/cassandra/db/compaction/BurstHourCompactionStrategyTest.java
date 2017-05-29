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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.tools.ant.taskdefs.Local;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Created by Pedro Gordo on 27/05/17.
 */
public class BurstHourCompactionStrategyTest
{
    private static final String keyspaceName = "TestingBhcsKeyspace";
    private static final String tableName = "TestingBhcsTable";
    private static final String sizeOptionkey = "sstable_max_size";
    private static final String sizeOptionValue = "30";
    private static ColumnFamilyStore cfs;
    private static CompactionStrategyManager compactionStrategyManager;
    private static Set<AbstractCompactionStrategy> strategies;
    private static int gcBefore;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();

        Map<String, String> options = new HashMap<>();
        options.put(sizeOptionkey, sizeOptionValue);

        CompactionParams compactionParams = CompactionParams.bhcs(options);

        SchemaLoader.createKeyspace(keyspaceName,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(keyspaceName, tableName)
                                                .compaction(compactionParams));
        cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);
        compactionStrategyManager = cfs.getCompactionStrategyManager();

        strategies = new HashSet<>();
        strategies.addAll(compactionStrategyManager.getUnrepaired());
        strategies.addAll(compactionStrategyManager.getRepaired());

        gcBefore = CompactionManager.getDefaultGcBefore(cfs, FBUtilities.nowInSeconds());
    }

    @Before
    public void createData()
    {
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 40;
        int columns = 20;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }
    }

    @After
    public void deleteData()
    {
        cfs.truncateBlocking();
    }

    @Test
    public void getNextBackgroundTaskOutsideBurstHour() throws Exception
    {
        LocalTime start = LocalTime.now().plusHours(1);
        LocalTime end = LocalTime.now().plusHours(2);
        setBurstHour(start, end);
        for(AbstractCompactionStrategy strategy : strategies)
        {
            assertNull(strategy.getNextBackgroundTask(gcBefore));
        }
    }

    @Test
    public void getNextBackgroundTaskDuringBurstHour() throws Exception
    {
        LocalTime start = LocalTime.now().minusMinutes(30);
        LocalTime end = LocalTime.now().plusMinutes(30);
        setBurstHour(start, end);

        for(AbstractCompactionStrategy strategy : strategies)
        {
            assertNotNull(strategy.getNextBackgroundTask(gcBefore));
        }
    }

    @Test
    public void getNextBackgroundTaskNotEnoughReferences() throws Exception
    {
    }

    @Test
    public void getNextBackgroundTask() throws Exception
    {
    }

    @Test
    public void getMaximalTaskNotEnoughReferences() throws Exception
    {
    }

    @Test
    public void getMaximalTask() throws Exception
    {
    }

    @Test
    public void getUserDefinedTask() throws Exception
    {
    }

    @Test
    public void validateOptionsWrongValues() throws Exception
    {
    }

    @Test
    public void validateOptionsWrongOptions() throws Exception
    {
    }

    @Test
    public void validateOptions() throws Exception
    {
    }

    private void setBurstHour(LocalTime start, LocalTime end)
    {
        for(AbstractCompactionStrategy compactionStrategy : strategies)
        {
            BurstHourCompactionStrategyOptions options = ((BurstHourCompactionStrategy) compactionStrategy).bhcsOptions;

            options.setStartTime(start);
            options.setEndTime(end);
        }
    }
}