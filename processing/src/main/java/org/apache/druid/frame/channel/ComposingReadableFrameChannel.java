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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.Frame;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Supplier;

public class ComposingReadableFrameChannel implements ReadableFrameChannel
{
  private final Supplier<ReadableFrameChannel>[] channels;
  private ReadableFrameChannel currentChannel;
  private int currentIndex;

  public ComposingReadableFrameChannel(
      int partition,
      Supplier<ReadableFrameChannel>[] channels,
      Map<Integer, HashSet<Integer>> partitionToChannelMap
  )
  {
    if (partitionToChannelMap.get(partition) == null) {
      // no writes for the partition, send an empty readable channel
      this.channels = new Supplier[]{() -> ReadableNilFrameChannel.INSTANCE};
    } else {
      HashSet<Integer> validChannels = partitionToChannelMap.get(partition);
      Preconditions.checkState(validChannels.size() > 0, "No channels found for partition " + partition);
      Supplier<ReadableFrameChannel>[] newChannels = new Supplier[validChannels.size()];
      ArrayList<Integer> sortedChannelIds = new ArrayList<>(validChannels);
      Collections.sort(sortedChannelIds); // the data was written from lowest to highest channel
      int idx = 0;
      for (Integer channelId : sortedChannelIds) {
        newChannels[idx++] = channels[channelId];
      }
      this.channels = newChannels;
    }
    this.currentIndex = 0;
    this.currentChannel = null;
  }

  @Override
  public boolean isFinished()
  {
    initCurrentChannel();
    if (!currentChannel.isFinished()) {
      return false;
    }
    currentChannel.close();
    currentChannel = null;
    if (isLastIndex()) {
      return true;
    }
    ++currentIndex;
    return isFinished();
  }

  @Override
  public boolean canRead()
  {
    initCurrentChannel();
    if (currentChannel.canRead()) {
      return true;
    }
    if (currentChannel.isFinished()) {
      currentChannel.close();
      currentChannel = null;
      if (isLastIndex()) {
        return false;
      }
      ++currentIndex;
      return canRead();
    }
    return false;
  }

  @Override
  public Frame read()
  {
    return currentChannel.read();
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    initCurrentChannel();
    if (!currentChannel.isFinished()) {
      return currentChannel.readabilityFuture();
    }
    currentChannel.close();
    currentChannel = null;
    if (isLastIndex()) {
      return Futures.immediateFuture(true);
    }
    ++currentIndex;
    return readabilityFuture();
  }

  @Override
  public void close()
  {
    if (currentChannel != null) {
      currentChannel.close();
    }
  }

  private boolean isLastIndex()
  {
    return currentIndex == channels.length - 1;
  }

  private void initCurrentChannel()
  {
    if (currentChannel == null) {
      currentChannel = channels[currentIndex].get();
    }
  }
}