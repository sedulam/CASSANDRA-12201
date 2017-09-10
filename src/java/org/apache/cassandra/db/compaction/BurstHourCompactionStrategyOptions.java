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
import java.time.format.DateTimeParseException;
import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Custom options for {@link BurstHourCompactionStrategy}:
 * <ul>
 *     <li>{@link BurstHourCompactionStrategyOptions#startTime}: The beginning of the time period where compaction can
 *     occur.</li>
 *     <li>{@link BurstHourCompactionStrategyOptions#endTime}: The ending of the time period where compaction can
 *     occur.</li>
 *     <li>{@link BurstHourCompactionStrategyOptions#sstableMaxSize}: maximum size that each produced compacted table
 *     can have.</li>
 * </ul>
 */
class BurstHourCompactionStrategyOptions
{
    private static final LocalTime defaultStartTime = LocalTime.MIDNIGHT;
    static final String START_TIME_KEY = "start_time";
    private LocalTime startTime;
    private static final LocalTime defaultEndTime = LocalTime.MIDNIGHT.plusHours(1);
    static final String END_TIME_KEY = "end_time";
    private LocalTime endTime;
    private static final int SSTABLE_MAX_SIZE_DEFAULT = 30;
    static final String SSTABLE_MAX_SIZE_KEY = "sstable_max_size";
    final long sstableMaxSize;

    /**
     * Compaction used for {@link BurstHourCompactionStrategy}.
     * @param options for allowed options, relevant to this strategy,
     *                see {@link BurstHourCompactionStrategyOptions}
     */
    BurstHourCompactionStrategyOptions(Map<String, String> options)
    {
        if (options == null)
        {
            startTime = defaultStartTime;
            endTime = defaultEndTime;
            sstableMaxSize = SSTABLE_MAX_SIZE_DEFAULT;
        }
        else
        {
            startTime = options.get(START_TIME_KEY) == null ? defaultStartTime : LocalTime.parse(options.get(START_TIME_KEY));
            endTime = options.get(END_TIME_KEY) == null ? defaultEndTime : LocalTime.parse(options.get(END_TIME_KEY));
            sstableMaxSize = options.get(SSTABLE_MAX_SIZE_KEY) == null ? SSTABLE_MAX_SIZE_DEFAULT : Integer.parseInt(options.get(SSTABLE_MAX_SIZE_KEY));
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static Map<String,String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException
    {
        String textStartTime = options.get(START_TIME_KEY);
        String textEndTime = options.get(END_TIME_KEY);
        String textSstableMaxSize = options.get(SSTABLE_MAX_SIZE_KEY);
        try
        {
            if (textStartTime != null)
            {
                LocalTime.parse(textStartTime);
            }
            if (textEndTime != null)
            {
                LocalTime.parse(textEndTime);
            }
            if (textSstableMaxSize != null)
            {
                Integer.parseInt(textSstableMaxSize);
            }
        }
        catch (DateTimeParseException e)
        {
            throw new ConfigurationException("The value of " + e.getParsedString() + " could not be converted into a valid time", e);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("The value of " + textSstableMaxSize + " is not a valid integer.", e);
        }


        uncheckedOptions.remove(START_TIME_KEY);
        uncheckedOptions.remove(END_TIME_KEY);
        uncheckedOptions.remove(SSTABLE_MAX_SIZE_KEY);

        return uncheckedOptions;
    }

    public LocalTime getStartTime()
    {
        return startTime;
    }

    public void setStartTime(LocalTime startTime)
    {
        this.startTime = startTime;
    }

    LocalTime getEndTime()
    {
        return endTime;
    }

    void setEndTime(LocalTime endTime)
    {
        this.endTime = endTime;
    }
}

