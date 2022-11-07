package org.apache.druid.frame.channel;

import java.io.Closeable;

public interface PartitionedReadableFrameChannel extends Closeable
{
  ReadableFrameChannel openChannel(int partitionNumber);
}
