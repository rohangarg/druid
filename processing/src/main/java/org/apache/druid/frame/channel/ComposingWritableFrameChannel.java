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

package org.apache.druid.frame.channel;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

public class ComposingWritableFrameChannel implements WritableFrameChannel
{
  private final WritableFrameChannel[] channels;
  private final long[] limits;
  private final Map<Integer, HashSet<Integer>> partitionToChannelMap;
  private int currentIndex;
  private long currentChannelBytes;

  public ComposingWritableFrameChannel(
      WritableFrameChannel[] channels,
      long[] limits,
      Map<Integer, HashSet<Integer>> partitionToChannelMap
  )
  {
    this.channels = channels;
    this.limits = limits;
    this.currentIndex = 0;
    this.currentChannelBytes = 0;
    this.partitionToChannelMap = partitionToChannelMap;
  }

  @Override
  public void write(FrameWithPartition frameWithPartition) throws IOException
  {
    if (currentIndex >= channels.length) {
      throw new ISE("No more channels available to write. Total available channels : " + channels.length);
    }

    long frameBytes = frameWithPartition.frame().numBytes();
    if (currentIndex >= 0 && currentChannelBytes + frameBytes < limits[currentIndex]) {
      channels[currentIndex].write(frameWithPartition);
      currentChannelBytes += frameBytes;
      partitionToChannelMap.computeIfAbsent(frameWithPartition.partition(), k -> Sets.newHashSetWithExpectedSize(1))
                           .add(currentIndex);
    } else {
      channels[currentIndex].close();
      currentIndex++;
      currentChannelBytes = 0;
      // write empty partitions in new writable channel to maintain partition sanity in writers. may not be necessary
      /*for (int pNum = 0; pNum < frameWithPartition.partition(); pNum++) {
        if (frameWithPartition.frame().type() == FrameType.COLUMNAR) {
          write(new FrameWithPartition(Frame.EMPTY_COLUMNAR, pNum));
        } else if (frameWithPartition.frame().type() == FrameType.ROW_BASED) {
          write(new FrameWithPartition(Frame.EMPTY_ROW_BASED, pNum));
        } else {
          throw new IllegalArgumentException("Unknown frame type " + frameWithPartition.frame().type());
        }
      }*/
      write(frameWithPartition);
    }
  }

  @Override
  public void fail(@Nullable Throwable cause) throws IOException
  {
    for (WritableFrameChannel channel : channels) {
      channel.fail(cause);
    }
  }

  @Override
  public void close() throws IOException
  {
    if (currentIndex < channels.length) {
      channels[currentIndex].close();
    }
  }

  @Override
  public ListenableFuture<?> writabilityFuture()
  {
    if (currentIndex == channels.length - 1) {
      return channels[currentIndex].writabilityFuture();
    }
    // TODO : the constant should instead be max frame size. also this logic might leave a space of 8MB free in the current writer
    // doesn't seem like the best thing, need to check further
    if (currentChannelBytes + 8_000_000 < limits[currentIndex]) {
      return Futures.immediateFuture(true);
    }
    currentIndex++;
    return writabilityFuture();
  }
}
