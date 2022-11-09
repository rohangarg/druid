package org.apache.druid.frame.channel;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class ComposingWritableFrameChannel implements WritableFrameChannel
{
  private final WritableFrameChannel[] channels;
  private final long[] limits;
  private final AtomicReference<Map<Integer, HashSet<Integer>>> partitionToChannelMapRef;
  private int currentIndex;
  private long currentChannelBytes;

  public ComposingWritableFrameChannel(
      WritableFrameChannel[] channels,
      long[] limits,
      AtomicReference<Map<Integer, HashSet<Integer>>> partitionToChannelMapRef
  )
  {
    this.channels = channels;
    this.limits = limits;
    this.currentIndex = -1;
    this.currentChannelBytes = 0;
    this.partitionToChannelMapRef = partitionToChannelMapRef;
    this.partitionToChannelMapRef.set(new HashMap<>());
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
      partitionToChannelMapRef.get()
                              .computeIfAbsent(frameWithPartition.partition(), k -> Sets.newHashSetWithExpectedSize(1))
                              .add(currentIndex);
    } else {
      if (currentIndex >= 0 && currentIndex < channels.length) {
        channels[currentIndex].close();
      }
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
    if (currentIndex >= 0 && currentIndex < channels.length) {
      channels[currentIndex].close();
    }
  }

  @Override
  public ListenableFuture<?> writabilityFuture()
  {
//    if (currentIndex >= 0 && currentIndex == channels.length - 1) {
//      return channels[currentIndex].writabilityFuture();
//    }
    return Futures.immediateFuture(true);
  }
}
