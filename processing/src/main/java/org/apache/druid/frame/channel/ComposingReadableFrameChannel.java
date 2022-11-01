package org.apache.druid.frame.channel;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.Frame;

import java.util.function.Supplier;

public class ComposingReadableFrameChannel implements ReadableFrameChannel
{
  private final Supplier<ReadableFrameChannel>[] parts;
  private ReadableFrameChannel currentChannel;
  private int currentIndex;

  public ComposingReadableFrameChannel(Supplier<ReadableFrameChannel>[] parts)
  {
    this.parts = parts;
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
    return currentIndex == parts.length - 1;
  }

  private void initCurrentChannel()
  {
    if (currentChannel == null) {
      currentChannel = parts[currentIndex].get();
    }
  }
}
