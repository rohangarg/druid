package org.apache.druid.frame.processor;

import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.ComposingReadableFrameChannel;
import org.apache.druid.frame.channel.ComposingWritableFrameChannel;
import org.apache.druid.frame.channel.PartitionedReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    Map<Integer, HashSet<Integer>> partitionToChannelMap = new HashMap<>();
    ComposingWritableFrameChannel writableFrameChannel = new ComposingWritableFrameChannel(
        writableFrameChannels,
        limits,
        partitionToChannelMap
    );
    ComposingReadableFrameChannel readableFrameChannel = new ComposingReadableFrameChannel(
        partitionNumber,
        readableFrameChannelSuppliers,
        partitionToChannelMap
    );
    return OutputChannel.pair(
        writableFrameChannel,
        ArenaMemoryAllocator.createOnHeap(8_000_000),
        () -> readableFrameChannel,
        partitionNumber
    );
  }

  @Override
  public PartitionedOutputChannel openChannel(String name, boolean deleteAfterRead, long maxBytes) throws IOException
  {
    WritableFrameChannel[] writableFrameChannels = new WritableFrameChannel[limits.length];
    Supplier<PartitionedReadableFrameChannel>[] partitionedReaderSuppliers = new Supplier[limits.length];
    for (int i = 0; i < writableFrameChannels.length; i++) {
      PartitionedOutputChannel channel = parts.get(i).openChannel(name, deleteAfterRead, maxBytes);
      writableFrameChannels[i] = channel.getWritableChannel();
      partitionedReaderSuppliers[i] = channel.getReadableChannelSupplier();
    }
    Map<Integer, HashSet<Integer>> partitionToChannelMap = new HashMap<>();
    ComposingWritableFrameChannel writableFrameChannel = new ComposingWritableFrameChannel(
        writableFrameChannels,
        limits,
        partitionToChannelMap
    );
    PartitionedReadableFrameChannel partitionedReadableFrameChannel = new PartitionedReadableFrameChannel()
    {
      private final Set<Integer> openedChannels = Sets.newHashSetWithExpectedSize(1);

      @Override
      public ReadableFrameChannel openChannel(int partitionNumber)
      {
        Supplier<ReadableFrameChannel>[] suppliers = new Supplier[partitionedReaderSuppliers.length];
        for (int i = 0; i < partitionedReaderSuppliers.length; i++) {
          int finalI = i;
          suppliers[i] = Suppliers.memoize(
              () -> {
                openedChannels.add(finalI);
                return partitionedReaderSuppliers[finalI].get().openChannel(partitionNumber);
              }
          )::get;
        }
        return new ComposingReadableFrameChannel(partitionNumber, suppliers, partitionToChannelMap);
      }

      @Override
      public void close() throws IOException
      {
        for (Integer channelId : openedChannels) {
          partitionedReaderSuppliers[channelId].get().close();
        }
      }
    };

    return PartitionedOutputChannel.pair(
        writableFrameChannel,
        ArenaMemoryAllocator.createOnHeap(8_000_000),
        () -> partitionedReadableFrameChannel
    );
  }

  @Override
  public OutputChannel openNilChannel(int partitionNumber)
  {
    throw new UnsupportedOperationException();
  }
}
