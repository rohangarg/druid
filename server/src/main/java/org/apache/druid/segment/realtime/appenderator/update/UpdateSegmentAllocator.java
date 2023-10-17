/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.realtime.appenderator.update;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class UpdateSegmentAllocator implements SegmentAllocator
{
  private final String dataSource;
  private final AtomicBoolean invokedOnce;
  public static final String UPDATE_VERSION = String.valueOf(DateTimes.utc(System.currentTimeMillis()));
  public static final ShardSpec SHARD_SPEC = new NumberedShardSpec(0, 1);

  public UpdateSegmentAllocator(String dataSource)
  {
    this.dataSource = dataSource;
    this.invokedOnce = new AtomicBoolean(false);
  }

  @Nullable
  @Override
  public SegmentIdWithShardSpec allocate(
      InputRow row,
      String sequenceName,
      @Nullable String previousSegmentId,
      boolean skipSegmentLineageCheck
  ) throws IOException
  {
    if (invokedOnce.compareAndSet(false, true)) {
      return new SegmentIdWithShardSpec(
          dataSource,
          Intervals.ETERNITY,
          UPDATE_VERSION,
          SHARD_SPEC
      );
    }
    throw DruidException.defensive("Multiple invocations of the allocator. Not expected.");
  }
}
