package org.apache.druid.frame.processor;

import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.ComposingReadableFrameChannel;
import org.apache.druid.frame.channel.ComposingWritableFrameChannel;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

public class ComposingOutputChannelFactory implements OutputChannelFactory
{
  private final List<OutputChannelFactory> parts;
  private final long[] limits;

  public ComposingOutputChannelFactory(List<OutputChannelFactory> parts, long[] limits)
  {
    this.parts = parts;
    this.limits = limits;
  }

  @Override
  public OutputChannel openChannel(int partitionNumber) throws IOException
  {
    WritableFrameChannel[] writableFrameChannels = new WritableFrameChannel[limits.length];
    Supplier<ReadableFrameChannel>[] readableFrameChannelSuppliers = new Supplier[limits.length];
    for (int i = 0; i < writableFrameChannels.length; i++) {
      OutputChannel channel = parts.get(i).openChannel(partitionNumber);
      writableFrameChannels[i] = channel.getWritableChannel();
      readableFrameChannelSuppliers[i] = channel.getReadableChannelSupplier();
    }
    return createChannel(writableFrameChannels, readableFrameChannelSuppliers, partitionNumber);
  }

  @Override
  public OutputChannel openChannel(String name, boolean deleteAfterRead, long maxBytes) throws IOException
  {
    WritableFrameChannel[] writableFrameChannels = new WritableFrameChannel[limits.length];
    Supplier<ReadableFrameChannel>[] readableFrameChannelSuppliers = new Supplier[limits.length];
    for (int i = 0; i < writableFrameChannels.length; i++) {
      OutputChannel channel = parts.get(i).openChannel(name, deleteAfterRead, maxBytes);
      writableFrameChannels[i] = channel.getWritableChannel();
      readableFrameChannelSuppliers[i] = channel.getReadableChannelSupplier();
    }
    return createChannel(writableFrameChannels, readableFrameChannelSuppliers, FrameWithPartition.NO_PARTITION);
  }

  private OutputChannel createChannel(
      WritableFrameChannel[] writableFrameChannels,
      Supplier<ReadableFrameChannel>[] readableFrameChannelSuppliers,
      int partitionNumber
  )
  {
    ComposingWritableFrameChannel writableFrameChannel = new ComposingWritableFrameChannel(writableFrameChannels, limits);
    ComposingReadableFrameChannel readableFrameChannel = new ComposingReadableFrameChannel(readableFrameChannelSuppliers);
    return OutputChannel.pair(
        writableFrameChannel,
        ArenaMemoryAllocator.createOnHeap(8_000_000),
        () -> readableFrameChannel,
        partitionNumber
    );
  }

  @Override
  public OutputChannel openNilChannel(int partitionNumber)
  {
    throw new UnsupportedOperationException();
  }
}
