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
      Preconditions.checkState(validChannels.size() > 0, "No channels found for partition "  + partition);
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
    if (isLastIndex()) {
      return currentChannel.isFinished();
    }
    return false;
  }

  @Override
  public boolean canRead()
  {
    initCurrentChannel();
    if (isLastIndex()) {
      return currentChannel.canRead();
    }
    return true;
  }

  @Override
  public Frame read()
  {
    initCurrentChannel();
    if (currentChannel.canRead()) {
      return currentChannel.read();
    }
    currentChannel.close();
    if (isLastIndex()) {
      throw new RuntimeException();
    }
    ++currentIndex;
    return read();
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    return Futures.immediateFuture(true);
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
