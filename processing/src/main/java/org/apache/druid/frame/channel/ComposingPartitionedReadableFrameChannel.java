package org.apache.druid.frame.channel;

import com.google.common.base.Suppliers;

import java.util.HashSet;
import java.util.Map;
import java.util.function.Supplier;

public class ComposingPartitionedReadableFrameChannel implements PartitionedReadableFrameChannel
{
  private final Supplier<PartitionedReadableFrameChannel>[] parts;
  private final Map<Integer, HashSet<Integer>> partitionToChannelMap;

  public ComposingPartitionedReadableFrameChannel(
      Supplier<PartitionedReadableFrameChannel>[] parts,
      Map<Integer, HashSet<Integer>> partitionToChannelMap
  )
  {
    this.parts = parts;
    this.partitionToChannelMap = partitionToChannelMap;
  }

  @Override
  public ReadableFrameChannel openChannel(int partitionNumber)
  {
    // build an array of suppliers which will can supply a readable frame channel from a partitioned readable channel
    // for a given partition number
    Supplier<ReadableFrameChannel>[] readableFrameChannelSuppliers = new Supplier[parts.length];
    for (int i = 0; i < parts.length; i++) {
      int finalI = i;
      readableFrameChannelSuppliers[i] = Suppliers.memoize(() -> parts[finalI].get().openChannel(partitionNumber))::get;
    }
    return new ComposingReadableFrameChannel(partitionNumber, readableFrameChannelSuppliers, partitionToChannelMap);
  }
}
