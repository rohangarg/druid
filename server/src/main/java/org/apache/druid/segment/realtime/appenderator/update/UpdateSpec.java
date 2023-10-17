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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.incremental.AppendableIndexBuilder;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class UpdateSpec implements AppendableIndexSpec
{
  public static final String TYPE = "updatable";
  private final List<String> keyColumns;
  private final String versionColumn;
  private final int snapshotCountToRetain;

  public UpdateSpec()
  {
    this(ImmutableList.of(), null, null);
  }

  @JsonCreator
  public UpdateSpec(
      final @JsonProperty("keyColumns") List<String> keyColumns,
      final @JsonProperty("versionColumn") @Nullable String versionColumn,
      final @JsonProperty("snapshotCountToRetain") @Nullable Integer snapshotCountToRetain
  )
  {
    this.keyColumns = keyColumns;
    this.versionColumn = versionColumn == null ? ColumnHolder.TIME_COLUMN_NAME : versionColumn;
    this.snapshotCountToRetain = snapshotCountToRetain == null ? 25 : snapshotCountToRetain;
  }

  @JsonProperty
  public List<String> getKeyColumns()
  {
    return keyColumns;
  }

  @JsonProperty
  public String getVersionColumn()
  {
    return versionColumn;
  }

  @JsonProperty
  public Integer getSnapshotCountToRetain()
  {
    return snapshotCountToRetain;
  }

  @Override
  public AppendableIndexBuilder builder()
  {
    return new AppendableIndexBuilder()
    {
      @Override
      protected IncrementalIndex buildInner()
      {
        throw new UnsupportedOperationException("");
      }
    };
  }

  @Override
  public long getDefaultMaxBytesInMemory()
  {
    // We initially estimated this to be 1/3(max jvm memory), but bytesCurrentlyInMemory only
    // tracks active index and not the index being flushed to disk, to account for that
    // we halved default to 1/6(max jvm memory)
    return JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes() / 6;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UpdateSpec that = (UpdateSpec) o;
    return snapshotCountToRetain == that.snapshotCountToRetain
           && Objects.equals(keyColumns, that.keyColumns)
           && Objects.equals(versionColumn, that.versionColumn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(keyColumns, versionColumn, snapshotCountToRetain);
  }
}
