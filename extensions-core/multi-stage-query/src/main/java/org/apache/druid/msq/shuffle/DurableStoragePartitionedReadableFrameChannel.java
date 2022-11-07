package org.apache.druid.msq.shuffle;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.PartitionedReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableInputStreamFrameChannel;
import org.apache.druid.frame.file.FrameFileFooter;
import org.apache.druid.storage.StorageConnector;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

public class DurableStoragePartitionedReadableFrameChannel implements PartitionedReadableFrameChannel
{
  private final StorageConnector storageConnector;
  private final Supplier<FrameFileFooter> frameFileFooterSupplier;
  private final String frameFileFullPath;

  public DurableStoragePartitionedReadableFrameChannel(
      StorageConnector storageConnector,
      Supplier<FrameFileFooter> frameFileFooterSupplier,
      String frameFileFullPath
  )
  {
    this.storageConnector = storageConnector;
    this.frameFileFooterSupplier = frameFileFooterSupplier;
    this.frameFileFullPath = frameFileFullPath;


  }

  @Override
  public ReadableFrameChannel openChannel(int partitionNumber)
  {
    FrameFileFooter frameFileFooter = frameFileFooterSupplier.get();
    // find the range to read for partition
    long start = frameFileFooter.getPartitionStartFrame(partitionNumber);
    long end = frameFileFooter.getPartitionStartFrame(partitionNumber + 1);

    try {
      return ReadableInputStreamFrameChannel.open(
          storageConnector.rangeRead(frameFileFullPath, start, end - start + 1),
          frameFileFullPath,
          null
      );
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() throws IOException
  {
    try {
      storageConnector.deleteFile(frameFileFullPath);
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
