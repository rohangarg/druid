package org.apache.druid.frame.channel;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

public class ComposingWritableFrameChannel implements WritableFrameChannel
{
  private final WritableFrameChannel[] parts;
  private final long[] limits;
  private final Map<Integer, HashSet<Integer>> partitionToChannelMap;
  private int currentIndex;
  private long currentChannelBytes;

  public ComposingWritableFrameChannel(
      WritableFrameChannel[] parts,
      long[] limits,
      Map<Integer, HashSet<Integer>> partitionToChannelMap
  )
  {
    this.parts = parts;
    this.limits = limits;
    this.currentIndex = -1;
    this.currentChannelBytes = 0;
    this.partitionToChannelMap = partitionToChannelMap;
  }

  @Override
  public void write(FrameWithPartition frameWithPartition) throws IOException
  {
    if (currentIndex >= parts.length) {
      throw new RuntimeException("No more channels available to write. Total available channels : " + parts.length);
    }

    if (currentIndex >= 0 && currentChannelBytes < limits[currentIndex]) {
      parts[currentIndex].write(frameWithPartition);
      // TODO : write header size as well
      currentChannelBytes += frameWithPartition.frame().numBytes();
      partitionToChannelMap.computeIfAbsent(frameWithPartition.partition(), k -> Sets.newHashSet()).add(currentIndex);
    } else {
      currentIndex++;
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
    for (WritableFrameChannel composition : parts) {
      composition.fail(cause);
    }
  }

  @Override
  public void close() throws IOException
  {
    for (WritableFrameChannel composition : parts) {
      composition.close();
    }
  }

  @Override
  public ListenableFuture<?> writabilityFuture()
  {
    if (currentIndex > 0 && currentIndex == parts.length - 1) {
      return parts[currentIndex].writabilityFuture();
    }
    return Futures.immediateFuture(true);
  }
}
