package org.apache.druid.frame.channel;

public interface PartitionedReadableFrameChannel
{
  ReadableFrameChannel openChannel(int partitionNumber);
}
