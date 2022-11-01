package org.apache.druid.frame.channel;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;
import java.io.IOException;

public class ComposingWritableFrameChannel implements WritableFrameChannel
{
  private final WritableFrameChannel[] parts;
  private final long[] limits;
  private int currentIndex;
  private long currentChannelBytes;

  public ComposingWritableFrameChannel(WritableFrameChannel[] parts, long[] limits)
  {
    this.parts = parts;
    this.limits = limits;
    this.currentIndex = 0;
    this.currentChannelBytes = 0;
  }

  @Override
  public void write(FrameWithPartition frameWithPartition) throws IOException
  {
    if (currentIndex >= parts.length) {
      throw new RuntimeException();
    }

    if (currentChannelBytes < limits[currentIndex]) {
      parts[currentIndex].write(frameWithPartition);
      currentChannelBytes += frameWithPartition.frame().numBytes();
    } else {
      currentIndex++;
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
    if (currentIndex == parts.length - 1) {
      return parts[currentIndex].writabilityFuture();
    }
    return Futures.immediateFuture(true);
  }
}
